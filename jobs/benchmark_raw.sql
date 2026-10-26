SET TimeZone = 'UTC';
SET threads = 1;
SET memory_limit = '1024MB';
SET preserve_insertion_order = false;
.timer on
-- ЛР2, вариант 12, раздел 8. Контрольный час получения 19.10.2026 16:00 UTC.
-- Читаем только 36 gzip-файлов одного часа; поля агрегата совпадают с Parquet.
-- Пример запуска: docker compose run --rm -T duckdb -bail -f /dev/stdin < jobs/benchmark_raw.sql
SELECT date_trunc('hour', event_time) AS event_hour_utc,
       BOARDID,
       count(*) AS events,
       count(LAST) AS priced_events,
       round(avg(LAST), 6) AS avg_last
FROM (
  SELECT CAST(CAST(event_time AS TIMESTAMPTZ) AT TIME ZONE 'UTC' AS TIMESTAMP) AS event_time,
         BOARDID,
         LAST
  FROM read_json(
    '/data/raw/stream/dt=2026-10-19/hour=16/part-*.jsonl.gz',
    columns = {'event_time':'VARCHAR', 'BOARDID':'VARCHAR', 'LAST':'DOUBLE'},
    format = 'newline_delimited',
    hive_partitioning = false
  )
) AS same_events
GROUP BY 1, 2
ORDER BY 1, 2;
