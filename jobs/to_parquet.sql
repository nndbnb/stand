-- ЛР2 / вариант 12, раздел 7: ПЕРВЫЙ КОНТРОЛЬНЫЙ ПРОГОН Parquet v1.
-- Период: 2026-10-19 16:00–16:59 UTC по времени ПОЛУЧЕНИЯ.
-- Этот файл пока НЕ преобразует весь архив: после проверки его расширим
-- на остальные интервалы и добавим отдельную обработку raw.history (candles).
-- НЕ смешивать marketdata и свечи; не удалять data/raw.
--
-- docker compose run --rm -T duckdb -bail -f /dev/stdin < jobs/to_parquet.sql
--
-- Схема колонок read_json сформирована из schemas/event.avsc (v1).
-- Для полноты Avro v1 поле NUMTRADES намеренно исключено до версии v2.
-- Времена в исходном JSON — ISO 8601 UTC. CAST вместо TRY_CAST:
-- невалидные даты должны остановить работу, а не исчезнуть без отчёта.

SET TimeZone = 'UTC';
SET threads = 1;
SET memory_limit = '1024MB';
SET preserve_insertion_order = false;

COPY (
  WITH source_rows AS (
    SELECT *
    FROM read_json(
      '/data/raw/stream/dt=2026-10-19/hour=16/part-*.jsonl.gz',
      columns = {
      'event_key': 'VARCHAR',
      'event_time': 'VARCHAR',
      'received_at': 'VARCHAR',
      'source': 'VARCHAR',
      'SECID': 'VARCHAR',
      'BOARDID': 'VARCHAR',
      'BID': 'DOUBLE',
      'BIDDEPTH': 'BIGINT',
      'OFFER': 'DOUBLE',
      'OFFERDEPTH': 'BIGINT',
      'SPREAD': 'DOUBLE',
      'BIDDEPTHT': 'BIGINT',
      'OFFERDEPTHT': 'BIGINT',
      'OPEN': 'DOUBLE',
      'LOW': 'DOUBLE',
      'HIGH': 'DOUBLE',
      'LAST': 'DOUBLE',
      'LASTCHANGE': 'DOUBLE',
      'LASTCHANGEPRCNT': 'DOUBLE',
      'QTY': 'BIGINT',
      'VALUE': 'DOUBLE',
      'VALUE_USD': 'DOUBLE',
      'WAPRICE': 'DOUBLE',
      'LASTCNGTOLASTWAPRICE': 'DOUBLE',
      'WAPTOPREVWAPRICEPRCNT': 'DOUBLE',
      'WAPTOPREVWAPRICE': 'DOUBLE',
      'CLOSEPRICE': 'DOUBLE',
      'MARKETPRICETODAY': 'DOUBLE',
      'MARKETPRICE': 'DOUBLE',
      'LASTTOPREVPRICE': 'DOUBLE',
      'VOLTODAY': 'BIGINT',
      'VALTODAY': 'DOUBLE',
      'VALTODAY_USD': 'DOUBLE',
      'ETFSETTLEPRICE': 'DOUBLE',
      'TRADINGSTATUS': 'VARCHAR',
      'UPDATETIME': 'VARCHAR',
      'LASTBID': 'DOUBLE',
      'LASTOFFER': 'DOUBLE',
      'LCLOSEPRICE': 'DOUBLE',
      'LCURRENTPRICE': 'DOUBLE',
      'MARKETPRICE2': 'DOUBLE',
      'NUMBIDS': 'BIGINT',
      'NUMOFFERS': 'BIGINT',
      'CHANGE': 'DOUBLE',
      'TIME': 'VARCHAR',
      'HIGHBID': 'DOUBLE',
      'LOWOFFER': 'DOUBLE',
      'PRICEMINUSPREVWAPRICE': 'DOUBLE',
      'OPENPERIODPRICE': 'DOUBLE',
      'SEQNUM': 'BIGINT',
      'SYSTIME': 'VARCHAR',
      'CLOSINGAUCTIONPRICE': 'DOUBLE',
      'CLOSINGAUCTIONVOLUME': 'BIGINT',
      'ISSUECAPITALIZATION': 'DOUBLE',
      'ISSUECAPITALIZATION_UPDATETIME': 'VARCHAR',
      'ETFSETTLECURRENCY': 'VARCHAR',
      'VALTODAY_RUR': 'DOUBLE',
      'TRADINGSESSION': 'VARCHAR',
      'TRENDISSUECAPITALIZATION': 'DOUBLE'
      },
      format = 'newline_delimited',
      hive_partitioning = false
    )
  ), typed AS (
    SELECT
    "event_key",
    CAST(CAST("event_time" AS TIMESTAMPTZ) AT TIME ZONE 'UTC' AS TIMESTAMP_MS) AS event_time,
    CAST(CAST("received_at" AS TIMESTAMPTZ) AT TIME ZONE 'UTC' AS TIMESTAMP_MS) AS received_at,
    "source",
    "SECID",
    "BOARDID",
    "BID",
    "BIDDEPTH",
    "OFFER",
    "OFFERDEPTH",
    "SPREAD",
    "BIDDEPTHT",
    "OFFERDEPTHT",
    "OPEN",
    "LOW",
    "HIGH",
    "LAST",
    "LASTCHANGE",
    "LASTCHANGEPRCNT",
    "QTY",
    "VALUE",
    "VALUE_USD",
    "WAPRICE",
    "LASTCNGTOLASTWAPRICE",
    "WAPTOPREVWAPRICEPRCNT",
    "WAPTOPREVWAPRICE",
    "CLOSEPRICE",
    "MARKETPRICETODAY",
    "MARKETPRICE",
    "LASTTOPREVPRICE",
    "VOLTODAY",
    "VALTODAY",
    "VALTODAY_USD",
    "ETFSETTLEPRICE",
    "TRADINGSTATUS",
    "UPDATETIME",
    "LASTBID",
    "LASTOFFER",
    "LCLOSEPRICE",
    "LCURRENTPRICE",
    "MARKETPRICE2",
    "NUMBIDS",
    "NUMOFFERS",
    "CHANGE",
    "TIME",
    "HIGHBID",
    "LOWOFFER",
    "PRICEMINUSPREVWAPRICE",
    "OPENPERIODPRICE",
    "SEQNUM",
    "SYSTIME",
    "CLOSINGAUCTIONPRICE",
    "CLOSINGAUCTIONVOLUME",
    "ISSUECAPITALIZATION",
    "ISSUECAPITALIZATION_UPDATETIME",
    "ETFSETTLECURRENCY",
    "VALTODAY_RUR",
    "TRADINGSESSION",
    "TRENDISSUECAPITALIZATION"
    FROM source_rows
  )
  SELECT *, CAST(event_time AS DATE) AS event_date
  FROM typed
)
TO '/data/duckdb/parquet/events'
(
  FORMAT PARQUET,
  COMPRESSION ZSTD,
  PARTITION_BY (event_date),
  OVERWRITE_OR_IGNORE TRUE,
  FILENAME_PATTERN 'recv-20261019-16_{i}',
  ROW_GROUP_SIZE 100000
);

-- Контроль результата того же часа; expected rows = 61644.
SELECT count(*) AS parquet_rows,
       min(event_time) AS min_event_time_utc,
       max(event_time) AS max_event_time_utc,
       count(DISTINCT event_key) AS distinct_event_keys
FROM read_parquet(
 '/data/duckdb/parquet/events/event_date=*/recv-20261019-16_*.parquet',
 hive_partitioning = true
);

DESCRIBE SELECT * FROM read_parquet(
 '/data/duckdb/parquet/events/event_date=*/recv-20261019-16_*.parquet',
 hive_partitioning = true
);
