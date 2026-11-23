"""Insert observed Spark metrics in NOTES.md without appending duplicate sections."""
import argparse
import json
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parents[1]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("report", type=Path)
    doc = json.loads(p.parse_args().report.read_text())
    if "stages" in doc:
        kind = "aggregate"
        stages = doc["stages"]
        if not stages:
            raise SystemExit("Spark UI не вернул завершённых стадий")
        lines = [f"Сутки `{doc['day']}`, приложение `{doc['app']}`. Стадий записи: {len(stages)}. "
                 f"Счётчик Input: {doc['input_bytes']} байт. Данные суток найдены в {doc['day_files']} "
                 f"файлах из {doc['history_files']} файлов текущего снимка. Полный размер файлов "
                 f"с данными суток: {doc['day_file_bytes']} байт.",
                 "", "| Стадия | Задачи | Input, Б | Shuffle read, Б | Shuffle write, Б |",
                 "|---|---:|---:|---:|---:|"]
        for s in stages:
            lines.append("| " + " | ".join(str(s[k]) for k in
                         ("stageId", "numCompleteTasks", "inputBytes", "shuffleReadBytes", "shuffleWriteBytes")) + " |")
        lines.extend(["", "Строки физического плана со сканированием, фильтром и обменами:", "", "```text"])
        lines.extend(line for line in doc["plan"].splitlines()
                     if any(k in line.lower() for k in ("batchscan", "filter", "partitioning", "exchange")))
        lines.append("```")
    else:
        kind = "skew"
        lines = [f"Период {doc['start']} — {doc['end']}. Событий {doc['rows']}, тикеров {doc['keys']}. "
                 f"Лидер `{doc['top']['SECID']}`: {doc['top']['n']} событий, "
                 f"в {doc['top_to_mean']:.2f} раза больше среднего по тикеру.", "",
                 "| Соединение | Стадия | Время стадии, с | Медиана задачи, мс | Все задачи, мс |",
                 "|---|---:|---:|---:|---|"]
        for name in ("merge", "broadcast"):
            run = doc[name]
            lines.append(f"| {name} | {run['stage']} | {run['stage_seconds']} | "
                         f"{median(run['task_ms'])} | {run['task_ms']} |")
        hot = doc["merge_hot_partition"]
        lines.extend(["", f"Hash-партиция самого частого тикера при SortMergeJoin: {hot}.", "",
                      "| Соединение | Партиция задачи | Строк | Длительность, мс |",
                      "|---|---:|---:|---:|"])
        for name in ("merge", "broadcast"):
            for t in sorted(doc[name]["tasks"], key=lambda t: t["partition"]):
                lines.append(f"| {name} | {t['partition']} | {t['rows']} | {t['ms']} |")
    notes = ROOT / "NOTES.md"
    start, end = f"<!-- lr7-{kind} -->", f"<!-- /lr7-{kind} -->"
    before, body = notes.read_text().split(start, 1)
    _, after = body.split(end, 1)
    notes.write_text(before + start + "\n" + "\n".join(lines) + "\n" + end + after)


if __name__ == "__main__":
    main()
