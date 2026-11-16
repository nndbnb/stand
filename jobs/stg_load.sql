-- Параметры ClickHouse: day, source_url, s3_key, s3_secret.
-- Запускает jobs/lr5.py. Сначала полностью готовится новая партиция.
TRUNCATE TABLE stg.events_load;
INSERT INTO stg.events_load (event_key,event_time,received_at,SECID,BOARDID,TRADINGSTATUS,LAST,BID,OFFER)
SELECT event_key, event_time, received_at, SECID, BOARDID,
       ifNull(TRADINGSTATUS, 'UNKNOWN'), LAST, BID, OFFER
FROM s3({source_url:String}, {s3_key:String}, {s3_secret:String}, 'Parquet')
WHERE event_time >= toDateTime64({day:Date},3,'UTC')
  AND event_time < toDateTime64(addDays({day:Date},1),3,'UTC');
-- Пустые сутки обрабатывает драйвер DROP PARTITION; дубли запрещены.
SELECT throwIf(count() != uniqExact(event_key), 'Duplicate source day')
FROM stg.events_load;
SELECT throwIf(countIf(BOARDID!='TQBR' OR event_key='' OR SECID='')>0, 'Unexpected source filter')
FROM stg.events_load;
ALTER TABLE stg.events REPLACE PARTITION {day:Date} FROM stg.events_load;
ALTER TABLE core.events REPLACE PARTITION {day:Date} FROM stg.events;
