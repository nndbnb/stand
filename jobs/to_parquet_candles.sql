-- ЛР2, вариант 12. Раздел 7: контрольная запись минутных свечей 02.10.2026.
-- Это отдельный набор parquet.candles, НЕ добавлять свечи в parquet.events.
-- Вход: raw.history/dt=2026-10-02/*.jsonl.gz (дата события UTC).
-- Запуск (важно: DuckDB 1.5.5 требует явный -f /dev/stdin):
-- docker compose run --rm -T duckdb -bail -f /dev/stdin < jobs/to_parquet_candles.sql
--
-- Подтверждённые поля свечи: open, close, high, low, value, volume,
-- begin, end, SECID, BOARDID, event_key, event_time, received_at, source.
-- begin/end оставлены исходными строками MSK, а event_time/received_at
-- явно приводятся к TIMESTAMP_MS в UTC.
-- Это пока один закрытый день. Полный архив выгрузим порциями после проверки.

SET TimeZone = 'UTC';
SET threads = 1;
SET memory_limit = '1024MB';
SET preserve_insertion_order = false;

COPY (
  WITH src AS (
    SELECT *
    FROM read_json(
      '/data/raw/history/dt=2026-10-02/candles-*.jsonl.gz',
      columns = {
        'open': 'DOUBLE',
        'close': 'DOUBLE',
        'high': 'DOUBLE',
        'low': 'DOUBLE',
        'value': 'DOUBLE',
        'volume': 'BIGINT',
        'begin': 'VARCHAR',
        'end': 'VARCHAR',
        'SECID': 'VARCHAR',
        'BOARDID': 'VARCHAR',
        'event_key': 'VARCHAR',
        'event_time': 'VARCHAR',
        'received_at': 'VARCHAR',
        'source': 'VARCHAR'
      },
      format = 'newline_delimited',
      hive_partitioning = false
    )
  ),
  typed AS (
    SELECT
      "open",
      "close",
      "high",
      "low",
      "value",
      "volume",
      "begin",
      "end",
      "SECID",
      "BOARDID",
      "event_key",
      CAST(CAST("event_time" AS TIMESTAMPTZ) AT TIME ZONE 'UTC' AS TIMESTAMP_MS) AS event_time,
      CAST(CAST("received_at" AS TIMESTAMPTZ) AT TIME ZONE 'UTC' AS TIMESTAMP_MS) AS received_at,
      "source"
    FROM src
  )
  SELECT *, CAST(event_time AS DATE) AS event_date
  FROM typed
)
TO '/data/duckdb/parquet/candles'
(
  FORMAT PARQUET,
  COMPRESSION ZSTD,
  PARTITION_BY (event_date),
  OVERWRITE_OR_IGNORE TRUE,
  FILENAME_PATTERN 'candles-20261002_{i}',
  ROW_GROUP_SIZE 100000
);

-- Проверка преобразования всего дня (ожидаемо 112622 строк по замерам ЛР1).
SELECT count(*) AS candle_rows,
       count(DISTINCT event_key) AS distinct_event_keys,
       min(event_time) AS min_utc,
       max(event_time) AS max_utc
FROM read_parquet(
  '/data/duckdb/parquet/candles/event_date=*/candles-20261002_*.parquet',
  hive_partitioning = true
);

-- Внутренняя Parquet-схема: физический/логический тип временных полей.
SELECT name, type, converted_type, logical_type
FROM parquet_schema(
  '/data/duckdb/parquet/candles/event_date=2026-10-02/candles-20261002_0.parquet'
)
WHERE name IN ('event_time', 'received_at');
