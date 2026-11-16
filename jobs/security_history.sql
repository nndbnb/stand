-- Пересборка истории из core по event_time, затем факты пересобираются целиком.
-- Если статусы не менялись ни у одной бумаги, используем суточную активность.
TRUNCATE TABLE core.security_changes;
INSERT INTO core.security_changes
WITH status_mode AS (
 SELECT max(versions) > 1 AS use_status FROM (
  SELECT SECID, uniqExact(TRADINGSTATUS) AS versions FROM core.events GROUP BY SECID
 )
), candidates AS (
 SELECT SECID,event_time AS valid_from,TRADINGSTATUS AS attribute,'trading_status' AS attribute_kind
 FROM core.events WHERE (SELECT use_status FROM status_mode)
 UNION ALL
 SELECT SECID,toDateTime64(event_date,3,'UTC') AS valid_from,
        concat(multiIf(count() < 100, 'low', count() < 1000, 'medium', 'high'), ':events=', toString(count())) AS attribute,
        'daily_activity' AS attribute_kind
 FROM core.events WHERE NOT (SELECT use_status FROM status_mode) GROUP BY SECID,event_date
), unique_times AS (
 SELECT SECID,valid_from,min(attribute) AS attribute,min(attribute_kind) AS attribute_kind
 FROM candidates GROUP BY SECID,valid_from
), marked AS (
 SELECT *,row_number() OVER w AS rn,lagInFrame(attribute,1,'') OVER w AS previous
 FROM unique_times WINDOW w AS (PARTITION BY SECID ORDER BY valid_from ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT SECID,valid_from,attribute,attribute_kind FROM marked WHERE rn=1 OR attribute!=previous;

TRUNCATE TABLE mart.dim_security;
INSERT INTO mart.dim_security
SELECT concat(SECID,'|',toString(valid_from)),SECID,attribute,attribute_kind,valid_from,
       leadInFrame(valid_from,1,toDateTime64('2299-12-31 00:00:00',3,'UTC')) OVER w AS valid_to,
       toUInt8(valid_to=toDateTime64('2299-12-31 00:00:00',3,'UTC')) AS is_current
FROM core.security_changes
WINDOW w AS (PARTITION BY SECID ORDER BY valid_from ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING);
TRUNCATE TABLE mart.dim_date;
INSERT INTO mart.dim_date SELECT DISTINCT event_date,toYear(event_date),toMonth(event_date),toDayOfWeek(event_date) FROM core.events;
TRUNCATE TABLE mart.dim_board;
INSERT INTO mart.dim_board SELECT DISTINCT BOARDID,'MOEX equities, main trading board' FROM core.events;
