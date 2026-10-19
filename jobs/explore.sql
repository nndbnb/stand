-- ЛР2, вариант 12. Раздел 6: типы и качество сырых данных MOEX ISS.
-- Выполнять из каталога ~/stand:
-- docker compose run --rm -T duckdb < jobs/explore.sql
-- Для raw.stream отключаем hive_partitioning: dt/hour — время получения UTC,
-- а не время event_time.

-- 1. Типы, автоматически определённые DuckDB.
DESCRIBE SELECT * FROM read_json_auto(
  '/data/raw/stream/**/*.jsonl.gz',
  union_by_name = true,
  hive_partitioning = false
);

-- 2. Число сохранённых событий по часам времени получения UTC.
SELECT date_trunc('hour', TRY_CAST(received_at AS TIMESTAMPTZ)) AS received_hour,
       count(*) AS events
FROM read_json_auto(
  '/data/raw/stream/**/*.jsonl.gz',
  union_by_name = true,
  hive_partitioning = false
)
GROUP BY 1
ORDER BY 1;

-- 3. Число значений event_key, которые встречаются более одного раза.
SELECT count(*) AS duplicated_keys
FROM (
  SELECT event_key
  FROM read_json_auto(
    '/data/raw/stream/**/*.jsonl.gz',
    union_by_name = true,
    hive_partitioning = false
  )
  GROUP BY event_key
  HAVING count(*) > 1
);

-- 4. Число событий с некорректным временем event_time.
SELECT count(*) AS bad_event_time
FROM read_json_auto(
  '/data/raw/stream/**/*.jsonl.gz',
  union_by_name = true,
  hive_partitioning = false
)
WHERE TRY_CAST(event_time AS TIMESTAMPTZ) IS NULL;
