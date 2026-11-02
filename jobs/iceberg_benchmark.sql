-- ЛР4: те же показатели, что в ЛР3, но по таблице Iceberg.
-- Запуск: docker compose run --rm -T duckdb -init init.sql -bail -f /dev/stdin < jobs/iceberg_benchmark.sql
SET TimeZone = 'UTC';
EXPLAIN ANALYZE
SELECT COUNT(*) AS rows, AVG(LAST) AS avg_last
FROM lake.clean.events;
EXPLAIN ANALYZE
SELECT COUNT(*) AS rows, AVG(LAST) AS avg_last
FROM lake.clean.events
WHERE event_time >= TIMESTAMP '2026-10-19 00:00:00'
  AND event_time < TIMESTAMP '2026-10-20 00:00:00';

-- Проверка состава файлов до компакции.
SELECT * FROM iceberg_metadata('lake.clean.events') LIMIT 5;
