-- ЛР2 / вариант 12, раздел 6. Исследование JSONL.gz через DuckDB 1.5.5.
-- Исправлено после завершения с кодом 137 (вероятное OOM): не читать сразу
-- всё дерево raw.stream; берём контрольный час Avro v1.
--
-- Запуск:
-- docker compose run --rm -T duckdb -bail -f /dev/stdin < jobs/explore.sql
--
-- Папка dt/hour означает UTC-время ПОЛУЧЕНИЯ, а не event_time.
-- Позднее аналогичные проверки можно повторить для других часов.

SET threads = 1;
SET memory_limit = '1024MB';
SET preserve_insertion_order = false;

SELECT 'UTC-час получения: 2026-10-19 16:00–16:59' AS scope;

-- 1. Автоопределённые типы JSON за контрольный час; типы только для выборки.
-- ВНИМАНИЕ: поля, содержащие только NULL, DuckDB не может достоверно типизировать.
DESCRIBE SELECT * FROM read_json_auto(
  '/data/raw/stream/dt=2026-10-19/hour=16/part-*.jsonl.gz',
  sample_size = 1000,
  hive_partitioning = false
);

-- Последующие проверки используют только три колонки, нужные для контроля.
-- Явная проекция позволяет не разбирать в память десятки рыночных показателей.

-- 2. Количество сохранённых событий по часу получения (должно совпасть
-- с числом 61644, проверенным fastavro, если файлы не менялись).
SELECT date_trunc('hour', TRY_CAST(received_at AS TIMESTAMPTZ)) AS received_hour,
       count(*) AS events
FROM read_json(
  '/data/raw/stream/dt=2026-10-19/hour=16/part-*.jsonl.gz',
  columns = {event_key: 'VARCHAR', event_time: 'VARCHAR', received_at: 'VARCHAR'},
  format = 'newline_delimited',
  hive_partitioning = false
)
GROUP BY 1
ORDER BY 1;

-- 3. Повторяющиеся значения event_key: число ключей и число лишних строк.
SELECT count(*) AS duplicated_keys,
       coalesce(sum(n - 1), 0) AS duplicate_rows
FROM (
  SELECT event_key, count(*) AS n
  FROM read_json(
    '/data/raw/stream/dt=2026-10-19/hour=16/part-*.jsonl.gz',
    columns = {event_key: 'VARCHAR', event_time: 'VARCHAR', received_at: 'VARCHAR'},
    format = 'newline_delimited',
    hive_partitioning = false
  )
  WHERE event_key IS NOT NULL
  GROUP BY event_key
  HAVING count(*) > 1
);

-- 4. Некорректное/отсутствующее время event_time и пустой ключ.
SELECT count(*) AS total_rows,
       count(*) FILTER (WHERE TRY_CAST(event_time AS TIMESTAMPTZ) IS NULL) AS bad_event_time,
       count(*) FILTER (WHERE event_key IS NULL OR event_key = '') AS missing_event_key
FROM read_json(
  '/data/raw/stream/dt=2026-10-19/hour=16/part-*.jsonl.gz',
  columns = {event_key: 'VARCHAR', event_time: 'VARCHAR', received_at: 'VARCHAR'},
  format = 'newline_delimited',
  hive_partitioning = false
);
