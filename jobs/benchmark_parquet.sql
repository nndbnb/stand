SET TimeZone = 'UTC';
SET threads = 1;
SET memory_limit = '1024MB';
SET preserve_insertion_order = false;
.timer on
-- ЛР2, вариант 12, раздел 8. ТЕ ЖЕ события, что в benchmark_raw.sql.
-- Партиция может отличаться от даты/часа получения: выборка по имени файла!
-- Пример запуска: docker compose run --rm -T duckdb -bail -f /dev/stdin < jobs/benchmark_parquet.sql
SELECT date_trunc('hour', event_time) AS event_hour_utc,
       BOARDID,
       count(*) AS events,
       count(LAST) AS priced_events,
       round(avg(LAST), 6) AS avg_last
FROM (
  SELECT event_time, BOARDID, LAST
  FROM read_parquet(
    '/data/duckdb/parquet/events/event_date=*/recv-20261019-16_*.parquet',
    hive_partitioning = true,
    union_by_name = true
  )
) AS same_events
GROUP BY 1, 2
ORDER BY 1, 2;
