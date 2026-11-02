-- ЛР4. Проверки Spark перед изменяющими операциями.
-- docker compose run --rm -T spark spark-sql -f /opt/jobs/maintenance.sql

SELECT snapshot_id, committed_at, operation
FROM lake.clean.events.snapshots ORDER BY committed_at;

SELECT COUNT(*) AS data_files,
       SUM(file_size_in_bytes) AS total_data_bytes,
       AVG(file_size_in_bytes) AS mean_data_file_bytes
FROM lake.clean.events.files;

SELECT partition, COUNT(*) AS data_files,
       SUM(file_size_in_bytes) AS total_bytes,
       AVG(file_size_in_bytes) AS average_file_bytes
FROM lake.clean.events.files
GROUP BY partition ORDER BY partition;

-- СНАЧАЛА показать прошлый снимок и выполнить rollback.
-- Только затем выполнять rewrite_data_files и expire_snapshots.
-- Ниже команды для пошагового исполнения, в этот диагностический файл НЕ включены:
-- SELECT COUNT(*) FROM lake.clean.events VERSION AS OF <snapshot_id>;
-- CALL lake.system.rollback_to_snapshot('clean.events', <snapshot_id>);
-- CALL lake.system.rewrite_data_files(table => 'clean.events');
-- CALL lake.system.expire_snapshots(table => 'clean.events',
--   older_than => TIMESTAMP '<future-cutoff>', retain_last => 1);
-- ALTER TABLE lake.clean.events ADD COLUMN NUMTRADES BIGINT;
