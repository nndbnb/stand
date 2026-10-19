#!/usr/bin/env python3
"""ЛР1, вариант 12: MOEX ISS TQBR -> JSONL/gzip.

Режимы: record (поток) и backfill (минутные свечи).
Оболочка курса передаёт режим, COLLECTOR_RAW_DIR и COLLECTOR_OUT_DIR.
"""

from __future__ import annotations

import gzip
import json
import os
import re
import shutil
import signal
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests

UTC = timezone.utc
MOSCOW = ZoneInfo("Europe/Moscow")
STOP = threading.Event()


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def log(level: str, msg: str, **fields: object) -> None:
    print(json.dumps({"ts": iso(utc_now()), "level": level, "msg": msg, **fields},
                     ensure_ascii=False, separators=(",", ":")), flush=True)


def parse_moscow(value: object) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("пустое время MOEX")
    parsed = datetime.fromisoformat(value.strip().replace(" ", "T"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=MOSCOW)
    return parsed.astimezone(UTC)


def atomic_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    try:
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def sync_dir(directory: Path) -> None:
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


@dataclass(frozen=True)
class Config:
    source: str
    board: str
    base_url: str
    user_agent: str
    raw_dir: Path
    out_dir: Path
    poll_seconds: float
    raw_limit: int
    segment_bytes: int

    @classmethod
    def from_env(cls, mode: str) -> Config:
        source = os.environ.get("SOURCE", "").strip()
        parts = os.environ.get("SOURCE_FILTER", "").strip().split("/")
        if source != "moex" or len(parts) != 3 or parts != ["stock", "shares", "TQBR"]:
            raise ValueError("для варианта 12 нужны SOURCE=moex и SOURCE_FILTER=stock/shares/TQBR")
        url = os.environ.get("COLLECTOR_ISS_BASE_URL", "").strip().rstrip("/")
        agent = os.environ.get("COLLECTOR_USER_AGENT", "").strip()
        if not url.startswith("https://") or not agent or "@" not in agent:
            raise ValueError("задайте COLLECTOR_ISS_BASE_URL=https://iss.moex.com/iss "
                             "и COLLECTOR_USER_AGENT с действующим email")
        raw = Path(os.environ.get("COLLECTOR_RAW_DIR", "/data/raw"))
        out = Path(os.environ.get("COLLECTOR_OUT_DIR", str(raw / ("history" if mode == "backfill" else "stream"))))
        poll = float(os.environ.get("COLLECTOR_POLL_SECONDS", "10"))
        if poll < 10:
            raise ValueError("для TQBR COLLECTOR_POLL_SECONDS должен быть не меньше 10")
        limit = int(os.environ.get("COLLECTOR_RAW_LIMIT_GB", "20"))
        segment = int(os.environ.get("COLLECTOR_SEGMENT_MB", "2"))
        if limit <= 0 or segment <= 0:
            raise ValueError("лимиты объёма должны быть положительными")
        return cls(source, parts[2], url, agent, raw, out, poll,
                   limit * 1_000_000_000, segment * 1024**2)

    @property
    def board_url(self) -> str:
        return f"{self.base_url}/engines/stock/markets/shares/boards/{self.board}/securities.json"

    def candles_url(self, ticker: str) -> str:
        return f"{self.base_url}/engines/stock/markets/shares/boards/{self.board}/securities/{quote(ticker, safe='')}/candles.json"


class Metrics:
    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.events = 0
        self.errors: dict[str, int] = {}
        self.last_success = 0.0
        self.lock = threading.Lock()

    def success(self, count: int) -> None:
        with self.lock:
            self.events += count
            self.last_success = time.time()

    def error(self, kind: str) -> None:
        with self.lock:
            self.errors[kind] = self.errors.get(kind, 0) + 1

    def render(self) -> bytes:
        with self.lock:
            lines = [f'collector_events_total{{mode="{self.mode}"}} {self.events}']
            lines += [f'collector_errors_total{{mode="{self.mode}",kind="{k}"}} {v}'
                      for k, v in sorted(self.errors.items())]
            lines.append(f"collector_last_success_timestamp_seconds {self.last_success:.3f}")
            return ("\n".join(lines) + "\n").encode()


def start_metrics_server(metrics: Metrics) -> None:
    port = int(os.environ.get("COLLECTOR_METRICS_PORT", "9101"))

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path != "/metrics":
                self.send_error(404)
                return
            content = metrics.render()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        def log_message(self, format: str, *args: object) -> None:
            return  # лог сборщика должен содержать только JSON

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log("info", "metrics_started", port=port)


class ISSClient:
    def __init__(self, config: Config, metrics: Metrics, delay: float = 0) -> None:
        self.config = config
        self.metrics = metrics
        self.delay = delay
        self.last_request_start = 0.0
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": config.user_agent, "Accept": "application/json"})

    def block(self, url: str, block: str, params: dict[str, object]) -> list[dict]:
        attempt = 0
        while not STOP.is_set():
            if self.delay:
                left = self.delay - (time.monotonic() - self.last_request_start)
                if left > 0 and STOP.wait(left):
                    break
            self.last_request_start = time.monotonic()
            attempt += 1
            try:
                # Ответ каждой страницы ограничен 16 МБ, архив целиком не загружаем.
                with self.session.get(url, params=params, timeout=(10, 45), stream=True) as response:
                    if response.status_code == 429 or response.status_code >= 500:
                        raise requests.HTTPError(f"HTTP {response.status_code}")
                    response.raise_for_status()
                    buffer = bytearray()
                    for chunk in response.iter_content(64 * 1024):
                        buffer.extend(chunk)
                        if len(buffer) > 16 * 1024 * 1024:
                            raise ValueError("страница ISS превышает 16 МБ")
                    document = json.loads(buffer)
                table = document.get(block)
                if not isinstance(table, dict) or not isinstance(table.get("columns"), list) or not isinstance(table.get("data"), list):
                    raise ValueError(f"в ответе нет таблицы {block}")
                cols = table["columns"]
                rows = table["data"]
                if any(len(row) != len(cols) for row in rows):
                    raise ValueError(f"размер строки {block} не соответствует columns")
                return [dict(zip(cols, row)) for row in rows]
            except (requests.RequestException, ValueError, json.JSONDecodeError) as exc:
                self.metrics.error("http" if isinstance(exc, requests.RequestException) else "format")
                # Постоянные 4xx, кроме 429, не следует повторять бесконечно.
                if isinstance(exc, requests.HTTPError) and exc.response is not None:
                    status = exc.response.status_code
                    if 400 <= status < 500 and status != 429:
                        raise
                pause = min(300, 2 ** min(attempt, 8))
                log("warn", "http", error=str(exc), attempt=attempt, pause=pause)
                if STOP.wait(pause):
                    break
                log("info", "retry", attempt=attempt + 1)
        raise InterruptedError("сборщик остановлен")

    def pages(self, url: str, block: str, fixed: dict[str, object]):
        """Итерация по страницам; допускает как limit=1000, так и ограничение 100."""
        start = 0
        previous: tuple[object, object, int] | None = None
        while not STOP.is_set():
            params = {"iss.meta": "off", "iss.only": block, "limit": 1000, "start": start, **fixed}
            rows = self.block(url, block, params)
            if not rows:
                return
            signature = (tuple(rows[0].items()), tuple(rows[-1].items()), len(rows))
            if signature == previous:
                raise ValueError(f"ISS повторил страницу {block}: start={start}")
            previous = signature
            yield start, rows
            start += len(rows)


class RawStorage:
    """Сжатые завершённые файлы; временный журнал в state/wal обеспечивает восстановление."""

    def __init__(self, config: Config) -> None:
        self.cfg = config
        self.wal_root = config.raw_dir / "state" / "wal"
        self.wal_root.mkdir(parents=True, exist_ok=True)
        config.out_dir.mkdir(parents=True, exist_ok=True)
        self.wal: Path | None = None
        self.hour: str | None = None

    def final_path(self, wal: Path) -> Path:
        relative = wal.relative_to(self.wal_root)
        return self.cfg.out_dir / relative.with_suffix(".jsonl.gz")

    def seal(self, wal: Path) -> None:
        target = self.final_path(wal)
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            tmp = target.with_name(target.name + ".tmp")
            try:
                with wal.open("rb") as source, tmp.open("wb") as raw_out:
                    with gzip.GzipFile(fileobj=raw_out, mode="wb", compresslevel=6, mtime=0) as packed:
                        shutil.copyfileobj(source, packed, length=128 * 1024)
                    raw_out.flush()
                    os.fsync(raw_out.fileno())
                os.replace(tmp, target)
                sync_dir(target.parent)
            finally:
                tmp.unlink(missing_ok=True)
        wal.unlink(missing_ok=True)
        log("info", "closed", file=str(target.relative_to(self.cfg.raw_dir)), bytes=target.stat().st_size)
        self.enforce_limit()

    def recover(self, last_by_sec: dict[str, str]) -> None:
        for wal in sorted(self.wal_root.rglob("*.jsonl")):
            # Восстанавливаем последнюю корректную строку и позиции событий.
            with wal.open("rb+") as file:
                good_end = 0
                while True:
                    line = file.readline()
                    if not line:
                        break
                    try:
                        row = json.loads(line)
                        secid = row["SECID"]
                        timestamp = row["event_time"]
                        if not line.endswith(b"\n"):
                            break
                        if timestamp > last_by_sec.get(secid, ""):
                            last_by_sec[secid] = timestamp
                        good_end = file.tell()
                    except (ValueError, KeyError, TypeError):
                        break
                file.truncate(good_end)
            if wal.stat().st_size:
                self.seal(wal)
            else:
                wal.unlink(missing_ok=True)
        self.enforce_limit()

    def append(self, received: datetime, events: list[dict]) -> None:
        if not events:
            return
        key = received.strftime("%Y-%m-%dT%H")
        if self.wal is not None and self.hour != key:
            self.close()
        if self.wal is None:
            relative = Path(f"dt={received:%Y-%m-%d}") / f"hour={received:%H}"
            self.wal = self.wal_root / relative / f"part-{received:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:12]}.jsonl"
            self.wal.parent.mkdir(parents=True, exist_ok=True)
            self.hour = key
        with self.wal.open("ab") as file:
            for event in events:
                file.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode() + b"\n")
            file.flush()
            os.fsync(file.fileno())
        if self.wal.stat().st_size >= self.cfg.segment_bytes:
            self.close()

    def close(self) -> None:
        if self.wal is not None:
            wal = self.wal
            self.wal = None
            self.hour = None
            self.seal(wal)

    def enforce_limit(self) -> None:
        completed = list((self.cfg.raw_dir / "stream").rglob("*.jsonl.gz")) + \
                    list((self.cfg.raw_dir / "history").rglob("*.jsonl.gz"))
        objects = [(p.stat().st_mtime_ns, p, p.stat().st_size) for p in completed if p.is_file()]
        total = sum(size for _, _, size in objects)
        for _, path, size in sorted(objects):
            if total <= self.cfg.raw_limit:
                break
            path.unlink(missing_ok=True)
            total -= size
            log("warn", "rotated", removed=str(path.relative_to(self.cfg.raw_dir)), bytes=size)


def record(config: Config, metrics: Metrics) -> None:
    state_path = config.raw_dir / "state" / "moex_record.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    if state_path.exists():
        with state_path.open(encoding="utf-8") as f:
            state = json.load(f)
        if state.get("source_filter") != "stock/shares/TQBR":
            raise ValueError("фильтр сохранённого состояния отличается от конфигурации")
        last: dict[str, str] = state.get("last_by_security", {})
    else:
        last = {}
        if any((config.raw_dir / "stream").rglob("*.jsonl.gz")):
            log("warn", "missing_state", note="проверьте сохранённое состояние перед запуском")
    store = RawStorage(config)
    store.recover(last)
    atomic_json(state_path, {"source_filter": "stock/shares/TQBR", "last_by_security": last})
    client = ISSClient(config, metrics)
    log("info", "start", mode="record", source=config.source, filter="stock/shares/TQBR")
    try:
        while not STOP.is_set():
            cycle_start = time.monotonic()
            got = kept = bad = 0
            if store.wal is not None and store.hour != utc_now().strftime("%Y-%m-%dT%H"):
                store.close()
            try:
                # У доски TQBR число бумаг порядка 500. Получаем marketdata ОДНИМ
                # запросом: отдельные ISS-маршруты игнорируют start для этого блока,
                # из-за чего постраничный обход зацикливается на повторе данных.
                # Постраничная загрузка остаётся для исторических свечей.
                rows = client.block(config.board_url, "marketdata", {
                    "iss.meta": "off", "iss.only": "marketdata", "limit": 1000,
                })
                got = len(rows)
                received = utc_now()
                events: list[dict] = []
                updates: dict[str, str] = {}
                for row in rows:
                    secid = row.get("SECID")
                    board = row.get("BOARDID", config.board)
                    if not isinstance(secid, str) or not secid or board != config.board:
                        continue
                    try:
                        event_time = iso(parse_moscow(row.get("SYSTIME")))
                    except (ValueError, TypeError):
                        bad += 1
                        continue
                    if event_time <= updates.get(secid, last.get(secid, "")):
                        continue
                    event = dict(row)
                    event.update({
                        "event_key": f"moex:{config.board}:{secid}:{event_time}",
                        "event_time": event_time,
                        "received_at": iso(received),
                        "source": config.source,
                    })
                    events.append(event)
                    updates[secid] = event_time
                store.append(received, events)  # fsync раньше обновления позиции
                last.update(updates)
                if updates:
                    atomic_json(state_path, {
                        "source_filter": "stock/shares/TQBR", "last_by_security": last,
                    })
                metrics.success(len(events))
                kept = len(events)
                log("info", "poll", got=got, kept=kept, bad_time=bad)
            except (requests.RequestException, ValueError) as exc:
                metrics.error("poll")
                log("error", "poll_failed", error=str(exc))
            left = config.poll_seconds - (time.monotonic() - cycle_start)
            if left > 0:
                STOP.wait(left)
    finally:
        store.close()
        atomic_json(state_path, {"source_filter": "stock/shares/TQBR", "last_by_security": last})
        log("info", "stopped", mode="record")


def write_history_file(target: Path, events: list[dict]) -> bool:
    if target.is_file():
        return False
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".tmp")
    try:
        with tmp.open("wb") as raw:
            with gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=6, mtime=0) as gz:
                for event in events:
                    gz.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode() + b"\n")
            raw.flush()
            os.fsync(raw.fileno())
        os.replace(tmp, target)
    finally:
        tmp.unlink(missing_ok=True)
    return True


def backfill(config: Config, metrics: Metrics) -> None:
    start_text = os.environ.get("COLLECTOR_HISTORY_FROM", "")
    end_text = os.environ.get("COLLECTOR_HISTORY_TO", "")
    start_day, end_day = date.fromisoformat(start_text), date.fromisoformat(end_text)
    if start_day > end_day:
        raise ValueError("COLLECTOR_HISTORY_FROM позже COLLECTOR_HISTORY_TO")
    client = ISSClient(config, metrics, delay=1.0)
    store = RawStorage(config)
    log("info", "start", mode="backfill", date_from=start_text, date_to=end_text)
    tickers: list[str] = []
    seen: set[str] = set()
    rows = client.block(config.board_url, "securities", {
        "iss.meta": "off", "iss.only": "securities", "limit": 1000,
    })
    for row in rows:
        symbol = row.get("SECID")
        if (isinstance(symbol, str) and symbol and symbol not in seen
                and row.get("BOARDID", config.board) == config.board):
            tickers.append(symbol)
            seen.add(symbol)
    if not tickers:
        raise ValueError("ISS не вернул ни одной бумаги TQBR")
    log("info", "securities", count=len(tickers))
    for i, symbol in enumerate(tickers, start=1):
        if STOP.is_set():
            break
        count = 0
        pages = 0
        url = config.candles_url(symbol)
        params = {"from": start_text, "till": end_text, "interval": 1}
        try:
            for offset, rows in client.pages(url, "candles", params):
                grouped: dict[str, list[dict]] = {}
                page_keys: set[str] = set()
                for row in rows:
                    try:
                        dt = parse_moscow(row.get("begin"))
                    except (ValueError, TypeError):
                        log("warn", "invalid_candle", ticker=symbol, begin=row.get("begin"))
                        continue
                    event_time = iso(dt)
                    event_key = f"moex:{config.board}:candle:{symbol}:{event_time}"
                    if event_key in page_keys:
                        continue
                    page_keys.add(event_key)
                    event = dict(row)
                    event.update({
                        "SECID": symbol, "BOARDID": config.board,
                        "event_key": event_key, "event_time": event_time,
                        "received_at": iso(utc_now()), "source": config.source,
                    })
                    grouped.setdefault(dt.date().isoformat(), []).append(event)
                safe_symbol = re.sub(r"[^a-zA-Z0-9_.-]", "_", symbol)
                for day, events in grouped.items():
                    target = config.out_dir / f"dt={day}" / f"candles-{safe_symbol}-page-{offset:06d}.jsonl.gz"
                    if write_history_file(target, events):
                        metrics.success(len(events))
                        count += len(events)
                pages += 1
                store.enforce_limit()
            log("info", "history_security", ticker=symbol, index=i,
                total=len(tickers), pages=pages, saved=count)
        except (requests.RequestException, ValueError) as exc:
            log("error", "history_failed", ticker=symbol, error=str(exc))
            raise  # не считаем загрузку успешной, если хотя бы одна бумага не обработана
    if STOP.is_set():
        log("warn", "backfill_interrupted", securities=len(tickers))
        raise InterruptedError("загрузка истории прервана; повторите backfill")
    log("info", "backfill_finished", securities=len(tickers), interrupted=False)


def handle_signal(_signum: int, _frame: object) -> None:
    STOP.set()


def main() -> int:
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    mode = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("COLLECTOR_MODE", "record")
    if mode not in {"record", "backfill"}:
        log("error", "unsupported_mode", mode=mode,
            note="produce и replay добавляются в ЛР6")
        return 2
    try:
        config = Config.from_env(mode)
        metrics = Metrics(mode)
        if mode == "record":
            start_metrics_server(metrics)
            record(config, metrics)
        else:
            backfill(config, metrics)
        return 0
    except (ValueError, OSError, requests.RequestException, InterruptedError) as exc:
        log("error", "fatal", mode=mode, error=str(exc))
        return 1


if __name__ == "__main__":
    sys.exit(main())
