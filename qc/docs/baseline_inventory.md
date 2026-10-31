# Baseline inventory | 2026-10-31 upgrade

- Existing, inspectable: Spark/Sedona silver pipeline, gold aggregations,
  Snowflake ingestion, Airflow DAG, SQL marts, Tableau Public link.
- Existing, requires original environment: /shared/data snapshots, Java/Spark
  runtime, GCP deployment, Snowflake credentials, Tableau workbook source.
- New, independently executable: Python/GeoPandas quality gate over two verified
  San Francisco public snapshots (2026-06-14, 2026-09-14), 2023 ACS five-year
  B19013 income and matching-vintage cartographic tract boundaries, snapshot
  accounting, spatial and Census audits, independent SQLite metric checks,
  idempotent partitions, fault injection, aggregate HTML report.

Original team attribution remains in the root README. The new slice uses a new
provider/vintage configuration; it is not evidence that the old VM ran or that
the original Tableau workbook has been validated. Changes to original Spark code
are reviewed statically; the executable local gate is validated independently.

Observed code issues addressed: negative Census special codes were stripped of
sign; host summaries mixed snapshots and counted a host's full portfolio in every
neighborhood. The new QC pipeline uses explicit snapshot/neighborhood denominators.
