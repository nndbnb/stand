-- ЛР2, вариант 12. Проверка совместного чтения старого Parquet v1 и новых суток v2.
-- После успешного jobs/export_v2_day.py:
-- docker compose run --rm -T duckdb -bail -f /dev/stdin < jobs/verify_v2_union.sql
SET TimeZone = 'UTC';
SET threads = 1;
SET memory_limit = '1024MB';
SET preserve_insertion_order = false;

-- Схема ОДНОГО старого файла: NUMTRADES физически отсутствует.
DESCRIBE SELECT * FROM read_parquet(
  '/data/duckdb/parquet/events/event_date=2026-10-19/recv-20261019-16_0.parquet',
  hive_partitioning = true
);

-- Схема нового файла (подставляется только новый день, записи v2).
DESCRIBE SELECT * FROM read_parquet(
  '/data/duckdb/parquet/events/event_date=*/v2-recv-*.parquet',
  hive_partitioning = true,
  union_by_name = true
);

-- Общая схема: NUMTRADES есть, в прежних файлах значение NULL.
DESCRIBE SELECT * FROM read_parquet(
  '/data/duckdb/parquet/events/event_date=*/*.parquet',
  hive_partitioning = true,
  union_by_name = true
);

-- По каждой дате показываем, сколько строк имеет новое поле.
SELECT event_date,
       count(*) AS rows,
       count(NUMTRADES) AS rows_with_numtrades
FROM read_parquet(
  '/data/duckdb/parquet/events/event_date=*/*.parquet',
  hive_partitioning = true,
  union_by_name = true
)
GROUP BY event_date
ORDER BY event_date;
