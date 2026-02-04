## Home Credit Data Engineering – Credit Risk Lakehouse

This repository contains a **banking credit risk analysis** project built on a **Lakehouse Medallion architecture (Bronze / Silver / Gold)** using the Home Credit Default Risk dataset.

The main goals are to:
- **Ingest raw banking-style data** into a Bronze layer (HDFS-style storage)
- **Clean, normalize, and join datasets** in a Silver layer using PySpark / Spark SQL
- **Produce analytical Gold tables and datamarts** (e.g. in PostgreSQL) for:
  - Credit risk indicators and default probabilities
  - Portfolio monitoring and exposure analysis
  - Management and regulatory-style reporting

High-level pipeline:
1. **Bronze** – raw CSV ingestion of all source files.
2. **Silver** – validation, enrichment, joins, window functions, and Parquet storage.
3. **Gold** – business KPIs, credit risk metrics, and reporting-friendly tables.

This repo is focused on the **engineering pipeline and architecture**, not on storing the raw data itself (see `.gitignore`).

