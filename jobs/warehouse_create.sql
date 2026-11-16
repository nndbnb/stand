-- ЛР5. Один факт = одно очищенное событие event_key, время UTC.
CREATE DATABASE IF NOT EXISTS stg;
CREATE DATABASE IF NOT EXISTS core;
CREATE DATABASE IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS stg.events
(
 event_key String, event_time DateTime64(3, 'UTC'), received_at Nullable(DateTime64(3, 'UTC')),
 SECID String, BOARDID LowCardinality(String), TRADINGSTATUS String,
 LAST Nullable(Float64), BID Nullable(Float64), OFFER Nullable(Float64),
 event_date Date MATERIALIZED toDate(event_time)
) ENGINE = MergeTree PARTITION BY event_date ORDER BY (SECID, event_time, event_key);
CREATE TABLE IF NOT EXISTS stg.events_load AS stg.events;
CREATE TABLE IF NOT EXISTS core.events AS stg.events;

CREATE TABLE IF NOT EXISTS core.security_changes
(
 SECID String, valid_from DateTime64(3, 'UTC'), attribute String, attribute_kind String
) ENGINE = MergeTree ORDER BY (SECID, valid_from);
CREATE TABLE IF NOT EXISTS mart.dim_security
(
 security_version_key String, SECID String, attribute String, attribute_kind String,
 valid_from DateTime64(3, 'UTC'), valid_to DateTime64(3, 'UTC'), is_current UInt8
) ENGINE = MergeTree ORDER BY (SECID, valid_from);
CREATE TABLE IF NOT EXISTS mart.dim_date
(
 date_key Date, year UInt16, month UInt8, weekday UInt8
) ENGINE = MergeTree ORDER BY date_key;
CREATE TABLE IF NOT EXISTS mart.dim_board
(
 board_key String, board_name String
) ENGINE = MergeTree ORDER BY board_key;
CREATE TABLE IF NOT EXISTS mart.fact_events
(
 event_key String, event_time DateTime64(3, 'UTC'), date_key Date,
 security_version_key String, SECID String, board_key String,
 last_price Nullable(Float64), bid Nullable(Float64), offer Nullable(Float64)
) ENGINE = MergeTree PARTITION BY date_key ORDER BY (SECID, event_time, event_key);
CREATE TABLE IF NOT EXISTS mart.events_hourly
(
 event_date Date, event_hour DateTime('UTC'), SECID String, events UInt64
) ENGINE = SummingMergeTree PARTITION BY event_date ORDER BY (event_hour, SECID);
CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_events_hourly TO mart.events_hourly AS
 SELECT date_key AS event_date, toStartOfHour(event_time) AS event_hour, SECID, count() AS events
 FROM mart.fact_events GROUP BY event_date, event_hour, SECID;

-- При повторной загрузке MV считает в изолированной таблице загрузки.
-- REPLACE PARTITION не запускает MV: публикуются ОБЕ рассчитанные партиции.
CREATE TABLE IF NOT EXISTS mart.fact_events_load AS mart.fact_events;
CREATE TABLE IF NOT EXISTS mart.events_hourly_load AS mart.events_hourly;
CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_events_hourly_load TO mart.events_hourly_load AS
 SELECT date_key AS event_date, toStartOfHour(event_time) AS event_hour, SECID, count() AS events
 FROM mart.fact_events_load GROUP BY event_date, event_hour, SECID;

CREATE OR REPLACE VIEW mart.dashboard_hourly AS
 SELECT toHour(event_hour) AS hour_utc, sum(events) AS events FROM mart.events_hourly GROUP BY hour_utc;
CREATE OR REPLACE VIEW mart.dashboard_top AS
 SELECT SECID, sum(events) AS events FROM mart.events_hourly GROUP BY SECID ORDER BY events DESC, SECID LIMIT 10;
CREATE OR REPLACE VIEW mart.dashboard_daily AS
 SELECT event_date, uniqExact(SECID) AS securities, sum(events) AS events
 FROM mart.events_hourly GROUP BY event_date;
