-- DAY=2026-10-19
-- ЛР3: очистка только одних UTC-суток. Генерируется/запускается jobs/clean.py.
-- Исходные файлы из s3://lake/raw/stream за D и D+1, только закрытые объекты.
-- Исторические candles НЕ входят в clean.events: это другой формат MOEX.
-- __RAW_FILES__, __DAY__, __OUT_OBJECT__ заменяет clean.py; вручную не запускать.
SET TimeZone = 'UTC';
SET threads = 2;
SET memory_limit = '2048MB';
SET preserve_insertion_order = false;

COPY (
WITH source_rows AS (
  SELECT * FROM read_json(
    [__RAW_FILES__],
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
    hive_partitioning = false,
    filename = true
  )
), typed AS (
  SELECT
    "event_key",
    CAST(TRY_CAST("event_time" AS TIMESTAMPTZ) AT TIME ZONE 'UTC' AS TIMESTAMP_MS) AS "event_time",
    CAST(TRY_CAST("received_at" AS TIMESTAMPTZ) AT TIME ZONE 'UTC' AS TIMESTAMP_MS) AS "received_at",
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
    "TRENDISSUECAPITALIZATION",
    filename AS _source_file
  FROM source_rows
), accepted AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY event_key
           ORDER BY received_at DESC NULLS LAST, _source_file DESC
         ) AS _rn
  FROM typed
  WHERE event_key IS NOT NULL AND event_key <> ''
    AND event_time IS NOT NULL
    AND SECID IS NOT NULL AND SECID <> ''
    AND BOARDID = 'TQBR'
    AND source = 'moex'
    AND event_time >= TIMESTAMP '__DAY__ 00:00:00'
    AND event_time < TIMESTAMP '__NEXT_DAY__ 00:00:00'
    -- Полностью пустые снимки без торговых показателей не пишем в clean.
    AND (LAST IS NOT NULL OR BID IS NOT NULL OR OFFER IS NOT NULL
         OR QTY IS NOT NULL OR VALUE IS NOT NULL)
)
SELECT
  "event_key",
  "event_time",
  "received_at",
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
  "TRENDISSUECAPITALIZATION",
  CAST(event_time AS DATE) AS event_date
FROM accepted
WHERE _rn = 1
)
TO '__OUT_OBJECT__'
(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 32768,
 OVERWRITE_OR_IGNORE TRUE);

SELECT COUNT(*) AS clean_rows,
       COUNT(DISTINCT event_key) AS distinct_keys,
       MIN(event_time) AS min_event_time_utc,
       MAX(event_time) AS max_event_time_utc
FROM read_parquet('__OUT_OBJECT__', hive_partitioning = true);
