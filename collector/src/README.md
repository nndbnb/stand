# Сборщик MOEX ISS — ЛР1, вариант 12

Точка входа: `main.py`. Режимы: `record` и `backfill`.
Код работает в готовом Docker-контейнере `collector` из репозитория курса.

## Переменные

| Переменная | Назначение | Значение |
|---|---|---|
| `SOURCE` | источник по варианту | `moex` |
| `SOURCE_FILTER` | доска | `stock/shares/TQBR` |
| `COLLECTOR_ISS_BASE_URL` | базовый адрес API | `https://iss.moex.com/iss` (обязателен) |
| `COLLECTOR_USER_AGENT` | идентификатор программы и действующий контакт | `BigDataLR1/1.0 (mailto:адрес)` (обязателен) |
| `COLLECTOR_POLL_SECONDS` | период опроса доски, секунд | `10` (не менее 10) |
| `COLLECTOR_RAW_LIMIT_GB` | предел закрытых сжатых файлов в `stream` и `history`, ГБ (10^9 байт) | `20` |
| `COLLECTOR_SEGMENT_MB` | размер временного файла записи до закрытия, МиБ | `2` |
| `COLLECTOR_HISTORY_FROM` | начало исторического периода (включительно) | `YYYY-MM-DD` |
| `COLLECTOR_HISTORY_TO` | конец исторического периода (включительно) | `YYYY-MM-DD` |
| `COLLECTOR_MODE` | режим по умолчанию | `record` |
| `COLLECTOR_RAW_DIR` | корень сырого слоя | задаёт оболочка: `/data/raw` |
| `COLLECTOR_OUT_DIR` | каталог режима | задаёт оболочка (`stream` или `history`) |
| `COLLECTOR_METRICS_PORT` | HTTP-порт счётчиков | задаёт оболочка: `9101` |

Адреса и контакт задаются через `.env`, не в коде. **Не коммитьте `.env`**.

## Запуск

```bash
docker compose build collector
docker compose --profile lab1 up -d
docker compose logs -f collector
docker compose run --rm collector backfill
```

## Раскладка

- Поток: `data/raw/stream/dt=YYYY-MM-DD/hour=HH/part-*.jsonl.gz` — по **времени получения UTC**.
- История: `data/raw/history/dt=YYYY-MM-DD/candles-<SECID>-page-*.jsonl.gz` — по **дате свечи UTC**.
- Временные файлы записи: `data/raw/state/wal/` (после восстановления превращаются в завершённые gzip-файлы).
- Сохранённая позиция: `data/raw/state/moex_record.json`.

Каждая запись содержит `event_key`, `event_time`, `received_at`, `source`; для истории `SECID` и поля свечи.
`SYSTIME` и `begin` MOEX трактуются как московское время и переводятся в UTC.
В `record` записываются новые пары `SECID` / `SYSTIME`; в `backfill` — минутные свечи с ключом `SECID` / `begin`.

Программа реализует повтор HTTP-запросов с увеличивающейся паузой, постраничную загрузку, JSON-журнал, ротацию и восстановление незакрытых файлов. При режиме `record` отдаёт `/metrics` на порту `9101` для дополнительного задания.

Режимы `produce` и `replay` относятся к ЛР6 и здесь пока не реализованы.

## Ограничения

- Для загрузки истории пользователь вручную задаёт период, включающий **семь торговых дней** до начала записи; сам сборщик не определяет биржевой календарь.
- Блок `securities` используется для списка бумаг, `marketdata` для потока, `candles` для истории.
- API и Docker из этой среды не запускались на реальном стенде; перед защитой требуется проверка на своей машине и ответах биржи.
- Метрики сбрасываются при перезапуске процесса, что допустимо для счётчиков Prometheus.
- Для работы без открытого терминала требуется настройка запуска WSL2 при входе Windows, как в методических указаниях.
