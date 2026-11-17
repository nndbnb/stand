-- ЛР5. Показать данные и слои ClickHouse. Только SELECT и SHOW.
-- Можно выполнять отдельные блоки в SQL-консоли.
-- Создание таблиц НЕ выполняется: DDL объясняется по jobs/warehouse_create.sql.

-- 1. Все три слоя.
SELECT name AS database
FROM system.databases
WHERE name IN ('stg', 'core', 'mart')
ORDER BY name;

-- 2. Все объекты трёх слоёв, включая загрузочные таблицы, MV и представления.
-- У View и MaterializedView нет собственных строк хранения: пустое значение
-- total_rows не означает, что запрос к ним вернёт пустой результат.
SELECT database, name AS object, engine, total_rows
FROM system.tables
WHERE database IN ('stg', 'core', 'mart')
ORDER BY database, name;

-- 3. Наполнены ли основные слои. Числа должны совпадать.
SELECT 'stg.events' AS table, count() AS events FROM stg.events
UNION ALL
SELECT 'core.events', count() FROM core.events
UNION ALL
SELECT 'mart.fact_events', count() FROM mart.fact_events;

-- 4. Что является фактом: пять снимков бумаги, а не пять отдельных сделок.
SELECT SECID, event_time, date_key, board_key, last_price, bid, offer
FROM mart.fact_events
LIMIT 5;

-- 5. Звезда: событие связано с конкретной версией бумаги, датой и доской.
SELECT f.SECID, f.event_time, f.date_key, f.board_key,
       d.attribute AS status_at_event, d.security_version_key
FROM mart.fact_events AS f
INNER JOIN mart.dim_security AS d USING (security_version_key)
INNER JOIN mart.dim_date AS c USING (date_key)
INNER JOIN mart.dim_board AS b USING (board_key)
LIMIT 5;

-- 6. Одна бумага может иметь несколько версий атрибутов.
SELECT SECID, attribute, valid_from, valid_to, is_current
FROM mart.dim_security
WHERE SECID = 'SMCF'
ORDER BY valid_from
LIMIT 8;

-- 7. Текущие движки, партиции и ключи сортировки основных физических таблиц.
SELECT database, name AS table, engine, partition_key, sorting_key
FROM system.tables
WHERE (database = 'stg' AND name = 'events')
   OR (database = 'core' AND name IN ('events', 'security_changes'))
   OR (database = 'mart' AND name IN
       ('fact_events', 'dim_security', 'dim_date', 'dim_board', 'events_hourly'))
ORDER BY database, name;

-- 8. Источники трёх графиков. Эти SELECT помогают объяснить столбцы в браузере.
SELECT hour_utc, events FROM mart.dashboard_hourly ORDER BY hour_utc;
SELECT SECID, events FROM mart.dashboard_top ORDER BY events DESC, SECID;
SELECT event_date, securities FROM mart.dashboard_daily ORDER BY event_date;

-- 9. Фактический DDL: читаем определения, НЕ пересоздаём объекты.
-- Эти четыре команды удобнее запускать по одной при объяснении DDL.
SHOW CREATE TABLE stg.events FORMAT TSVRaw;
SHOW CREATE TABLE mart.dim_security FORMAT TSVRaw;
SHOW CREATE TABLE mart.fact_events FORMAT TSVRaw;
SHOW CREATE TABLE mart.mv_events_hourly_load FORMAT TSVRaw;
