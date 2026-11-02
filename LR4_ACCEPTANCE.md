# ЛР4 — краткий гайд приёмки (вариант 12)

**Что показываем:** SeaweedFS `clean/` → Lakekeeper REST Catalog → Iceberg `lake.clean.events` → Spark (запись/обслуживание) и DuckDB (аналитика). Эксперименты уже проведены; повторять разрушительные операции **не нужно**. Протокол: [NOTES.md](NOTES.md), описание набора: [CATALOG.md](CATALOG.md).

## 0. Запуск и подключение

```bash
cd ~/stand
source .venv/bin/activate
docker compose --profile lab4 up -d
docker compose ps
```

Ожидание: работают `s3`, `catalog` (Lakekeeper) и необходимые зависимости. `spark` и `duckdb` запускаются на один запрос, не как постоянные сервисы. Важная особенность образа — **указывать полный путь** `/opt/spark/bin/spark-sql`.

## 1. Показать Iceberg-таблицу и скрытые партиции

```bash
docker compose run --rm -T spark /opt/spark/bin/spark-sql \
  -e "DESCRIBE TABLE lake.clean.events;
      SELECT COUNT(*) AS total_rows,
             COUNT(DISTINCT event_key) AS unique_keys,
             COUNT(DISTINCT CAST(event_time AS DATE)) AS days
      FROM lake.clean.events;"
```

Контрольный результат: **7 814 151 строк**, столько же уникальных `event_key`, **14 дат**; партиционирование `days(event_time)`, а не отдельная пользовательская колонка `event_date`. Схема изначально имела **59 колонок Avro v1**, позже добавили `NUMTRADES BIGINT`.

**Как загружали:** `jobs/iceberg_create.sql` создал таблицу, `jobs/iceberg_load.py --all` выполнил 37 интервалов без ошибок. Для повторной загрузки интервала `jobs/iceberg_load.sql` делает `DELETE` + `INSERT` в транзакции. На приёмке достаточно показать код, не запускать загрузку заново.

## 2. Iceberg partition pruning и DuckDB

```bash
docker compose run --rm -T duckdb \
  -init init.sql -bail -f /dev/stdin \
  < jobs/iceberg_benchmark.sql \
  2>&1 | tee data/duckdb/lr4-acceptance-benchmark.log
```

Контрольные измерения 02.11.2026:

| Показатель | Без фильтра | За 19.10 |
|---|---:|---:|
| Строк | 7 814 151 | 724 665 |
| Файлов прочитано DuckDB | 19 | 2 |
| HTTP GET | 128 | 7 |
| HTTPFS in | 20.1 MiB | 4.1 MiB |
| Общее время | 0.288 с | 0.112 с |

Результаты времени и сетевого счётчика могут меняться. Фильтр по `event_time` использует скрытую Iceberg-партицию `days(event_time)`. Число файлов в плане DuckDB может отличаться от количества всех активных data-файлов Iceberg.

## 3. Снимки / time travel / rollback — объяснить по зафиксированному эксперименту

```bash
docker compose run --rm -T spark /opt/spark/bin/spark-sql \
  -e "SELECT snapshot_id, committed_at, operation
      FROM lake.clean.events.snapshots
      ORDER BY committed_at DESC;
      SELECT COUNT(*) AS snapshots_now
      FROM lake.clean.events.snapshots;"
```

**ВНИМАНИЕ:** сейчас ожидается **1 снимок** после `expire_snapshots`; старый snapshot_id уже не пригоден для повторного time travel.

Что реально проверили до очистки истории:
1. Перед ошибкой `snapshot_id=8202167199995084800`, строк **7 814 151**.
2. `jobs/iceberg_bad_insert.sql` намеренно дописал ещё **510** событий 17.10 → **7 814 661** строк, но только **7 814 151** уникальных ключей.
3. `SELECT COUNT(*) ... VERSION AS OF 8202167199995084800` прочитал старое состояние: **7 814 151**.
4. `CALL lake.system.rollback_to_snapshot('clean.events', 8202167199995084800)` вернул таблицу к **7 814 151** строкам и ключам.

Объяснить: Iceberg хранит историю версий таблицы в метаданных, старые снимки пригодны для чтения/отката **только пока не истекли**. **Не запускать заново** `jobs/iceberg_bad_insert.sql` и `rollback_to_snapshot`.

## 4. Компакция мелких файлов

```bash
docker compose run --rm -T spark /opt/spark/bin/spark-sql \
  -e "SELECT partition,
             COUNT(*) AS data_files,
             SUM(file_size_in_bytes) AS bytes
      FROM lake.clean.events.files
      GROUP BY partition
      ORDER BY partition;"
```

До компакции за 27.10 было **5 файлов, 3 459 547 байт**. `CALL lake.system.rewrite_data_files(table => 'clean.events')` переписал **5 файлов в 1**; после — **1 файл, 2 571 537 байт**, всего **16 активных файлов**. Число событий и ключей не изменилось. Показываем `data/duckdb/lr4-compaction.log` (если сохранился), но **не перезапускаем** компакцию для демонстрации.

## 5. Очистка старых снимков

Было **22 снимка**, затем выполнили:

```sql
CALL lake.system.expire_snapshots(
  table => 'clean.events',
  older_than => TIMESTAMP '2026-11-03 00:00:00',
  retain_last => 1
);
```

После — **1 снимок**, **7 814 151 строк**. Процедура вернула показатели `6 0 0 6 21 0`, включая 21 удалённый снимок (не путать с числом объектов S3 по всем префиксам). **Это уже выполнено: повторять очистку не надо.** На защите SQL не вставлять прямо в Bash; SQL выполнялся в Spark через `-e`.

## 6. Эволюция схемы — Avro v1 → v2

```bash
docker compose run --rm -T spark /opt/spark/bin/spark-sql \
  -e "SELECT COUNT(*) AS rows,
             COUNT(NUMTRADES) AS filled
      FROM lake.clean.events;
      SELECT COUNT(*) AS data_files,
             SUM(file_size_in_bytes) AS bytes
      FROM lake.clean.events.files;"

docker compose run --rm -T duckdb -init init.sql -bail \
  -c "SELECT COUNT(*) AS rows, COUNT(NUMTRADES) AS filled
      FROM lake.clean.events"
```

Итог в обоих движках: **7 814 151 строк, `COUNT(NUMTRADES)=0`**. `ALTER TABLE lake.clean.events ADD COLUMN NUMTRADES BIGINT` сработал; до и после — **16 data-файлов, 763 815 858 байт**. Это метаданная эволюция схемы без переписывания старых Parquet; отсутствующее поле на старых строках — `NULL`.

## Короткие ответы преподавателю

- **Iceberg — это не формат только Parquet?** Данные лежат в файлах Parquet, а Iceberg описывает таблицу через метаданные, снимки, манифесты и правила изменений.
- **Зачем Lakekeeper?** Общий REST-каталог для Spark и DuckDB, хранит и публикует метаданные таблиц; сам Parquet лежит в S3.
- **Что даёт скрытая партиция?** Отсечение ненужных дней без физической колонки `event_date` в схеме Iceberg.
- **Snapshot vs data file?** Снимок ссылается на набор актуальных файлов, а не копирует все строки.
- **Почему rollback возможен?** Предыдущие файлы и метаданные сохранялись в истории снимков до `expire_snapshots`.
- **Зачем compaction?** Меньше мелких файлов, накладных расходов чтения и метаданных; строки не меняются.
- **Что сделал ALTER?** Добавил nullable-колонку в метаданные; старые значения читаются как `NULL`.
- **Ограничение доказательств:** конфликт параллельных writers не демонстрировался; подсчёт всех S3-объектов до/после expiration отдельно не проводился.
