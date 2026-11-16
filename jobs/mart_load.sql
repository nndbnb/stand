-- День фактов и агрегат пересчитываются вместе в таблицах загрузки.
TRUNCATE TABLE mart.fact_events_load;
TRUNCATE TABLE mart.events_hourly_load;
INSERT INTO mart.fact_events_load
SELECT e.event_key,e.event_time,toDate(e.event_time),d.security_version_key,e.SECID,e.BOARDID,e.LAST,e.BID,e.OFFER
FROM (SELECT * FROM core.events WHERE event_date={day:Date}) e
ASOF INNER JOIN mart.dim_security d ON e.SECID=d.SECID AND e.event_time>=d.valid_from
WHERE e.event_time<d.valid_to;
SELECT throwIf(
 (SELECT count() FROM mart.fact_events_load)!=(SELECT count() FROM core.events WHERE event_date={day:Date}),
 'Missing dimension version');
ALTER TABLE mart.fact_events REPLACE PARTITION {day:Date} FROM mart.fact_events_load;
ALTER TABLE mart.events_hourly REPLACE PARTITION {day:Date} FROM mart.events_hourly_load;
