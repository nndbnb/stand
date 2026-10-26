-- ЛР2, вариант 12, раздел 10. Только НОВАЯ UTC-дата event_time, Avro v2.
-- SQL-шаблон исполняет jobs/export_v2_day.py по одному закрытому часу получения.
-- Параметры __...__ заменяются Python-подготовкой. Вручную этот файл не запускать.
-- Исходные data/raw не изменяются. Старая дата event_date не переписывается.
SET TimeZone = 'UTC';
SET threads = 1;
SET memory_limit = '1024MB';
SET preserve_insertion_order = false;

COPY (
  WITH src AS (
    SELECT *
    FROM read_json(
      '__SOURCE_GLOB__',
      columns = {
__COLUMN_MAP__
      },
      format = 'newline_delimited',
      hive_partitioning = false
    )
  ), typed AS (
    SELECT
__PROJECTION__
    FROM src
  )
  SELECT *, CAST(event_time AS DATE) AS event_date
  FROM typed
  WHERE event_time >= TIMESTAMP '__DAY__ 00:00:00'
    AND event_time < TIMESTAMP '__NEXT_DAY__ 00:00:00'
)
TO '__STAGING_OUTPUT__'
(
  FORMAT PARQUET,
  COMPRESSION ZSTD,
  PARTITION_BY (event_date),
  OVERWRITE_OR_IGNORE TRUE,
  FILENAME_PATTERN '__PREFIX___{i}',
  ROW_GROUP_SIZE 100000
);

COPY (
  SELECT count(*) AS row_count,
         count(DISTINCT event_key) AS distinct_keys,
         count(NUMTRADES) AS rows_with_numtrades,
         count(*) FILTER (
           WHERE event_key IS NULL OR event_time IS NULL OR
                 CAST(event_time AS DATE) <> DATE '__DAY__'
         ) AS invalid_rows
  FROM read_parquet(
    '__STAGING_OUTPUT__/event_date=__DAY__/__PREFIX___*.parquet',
    hive_partitioning = true
  )
)
TO '__AUDIT_FILE__' (HEADER TRUE, DELIMITER ',');
