## ЛР7. Суточный конвейер MOEX, время UTC

| Набор | Источник и обработка | Гранулярность / обновление | Потребитель |
|---|---|---|---|
| lake.clean.events_hourly (Iceberg) | Spark, jobs/aggregate.py, читает lake.clean.events за D | Одна строка на (event_date, event_hour, SECID). DATE-партиция D заменяется целиком | ClickHouse через lake_ice.`clean.events_hourly`, сверка DuckDB |
| mart.events_hourly (ClickHouse) | Dagster загружает данные Iceberg через DataLakeCatalog в промежуточную таблицу; проверяет группы и заменяет партицию | Тот же ключ и дата. Существующие графики ЛР5 используют events. Новые entities и qty_sum рассчитаны для дней, обработанных ЛР7 | BI и суточная сверка |

events — число очищенных снимков котировки, entities — число пар (BOARDID, SECID) в группе. В варианте с одной доской TQBR и группировкой по SECID это 1. qty_sum — сумма QTY по снимкам; NULL пропускается, для полностью пустой группы сумма 0. QTY относится к последней сделке, сумма по снимкам не является биржевым оборотом.

Dagster, lr7_daily, ежедневно 01:30 UTC за предыдущие сутки. Зависимости: raw/stream (D и D+1) → clean_parquet (DuckDB, clean.sql) → iceberg_events (DuckDB, iceberg_load.sql) → iceberg_hourly (Spark) → clickhouse_hourly (ClickHouse) → quality_checks. Журнал Dagster хранит дату партиции, предупреждения и ошибки качества. Политика и показ — в LR7_ACCEPTANCE.md.
