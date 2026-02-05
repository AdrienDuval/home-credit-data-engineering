1. Global architecture (final target)
1.1 Business context (what this architecture achieves)
The platform turns raw banking data into credit-risk insights:
•	client-level risk profile (who is risky, why)
•	portfolio risk view (how risky is the bank’s loan book)
•	data products exposed through datamarts + API + dashboards
1.2 Sources (two distinct systems as required)
Source A (Internal system): PostgreSQL
•	Represents the bank’s operational system
•	Stores loan application + client attributes
•	Data is structured and stable → best represented as relational
Source B (External & historical feeds): CSV
•	Represents external bureau and behavioral histories
•	Data arrives as batch files → typical “feed / extract” pattern
1.3 Processing stack (what each tool does)
Apache Spark (PySpark)
•	Distributed processing engine
•	Used for ingestion, validation, joins, aggregations, and window functions
HDFS (Data Lake)
•	Stores Bronze, Silver, and optionally Gold files
•	Enables partitioned, scalable storage
Hive Metastore
•	Catalog: registers datasets as tables (external/managed)
•	Enables SQL access and easier debugging
PostgreSQL (Datamarts)
•	Relational serving layer for:
o	API queries
o	visualization
•	Not used for heavy transformations (Spark does that)
API + Visualization
•	API: secure access and pagination (exam requirement)
•	Visualization: minimum 3 charts from datamarts
1.4 Architectural flow (data lifecycle)
(PostgreSQL Source A) + (CSV Source B)
              ↓
        BRONZE  (raw, traceable, re-runnable)
              ↓
        SILVER  (validated, unified, curated)
              ↓
         GOLD   (business KPIs, risk datasets)
              ↓
  PostgreSQL DATAMARTS (serving layer)
              ↓
     API (JWT + pagination) + Dashboard (3 charts)
________________________________________
2. Layer-by-layer technical plan
2.1 Source layer (exact scope and purpose)
Source A — PostgreSQL (internal banking system) ✅ already done
Role
•	System of record for loan applications
•	Represents the internal, stable banking dataset
Tables
•	raw.application_train
•	raw.application_test
Primary Key
•	SK_ID_CURR
Access method
•	Spark JDBC read:
o	spark.read.format("jdbc")...option("dbtable", "raw.application_train")
Outcome
•	You can query and validate data with SQL at any time
•	Provides a credible “database source” for the exam
What to show in demo
•	\dt raw.* listing
•	SELECT count(*)...
•	index presence (optional but nice)
________________________________________
Source B — CSV files (external/historical feeds)
Files
•	bureau.csv (external credit records)
•	bureau_balance.csv (monthly status of bureau credits)
•	installments_payments.csv (repayment history)
•	credit_card_balance.csv (monthly credit card snapshots)
•	POS_CASH_balance.csv (POS/cash loan snapshots)
•	previous_application.csv (past applications)
Role
•	External bureau + historical behavioral signals
•	Event-like / snapshot-like data → ideal as batch feeds
Read method
•	Spark CSV read:
o	spark.read.option("header","true").csv(path)
Outcome
•	Second source is clearly different from PostgreSQL
•	Enables join + aggregation + window function work
What to show in demo
•	folder structure with partitions in /raw/csv/...
•	sample spark.read...show(10)
________________________________________
3. Bronze layer — Raw data lake (HDFS)
3.1 Purpose (why Bronze exists)
•	Preserve raw fidelity (no cleaning, no loss)
•	Ensure traceability (auditable)
•	Enable reprocessing (delete silver/gold, replay bronze)
•	Provide raw input for Silver (cleaning happens later)
3.2 Storage layout (must match exam requirement)
Partition by ingestion date on BOTH raw and silver:
year=YYYY/month=MM/day=DD
Bronze HDFS paths (final)
/raw/postgres/application_train/year=YYYY/month=MM/day=DD/
/raw/postgres/application_test/year=YYYY/month=MM/day=DD/

/raw/csv/bureau/year=YYYY/month=MM/day=DD/
/raw/csv/bureau_balance/year=YYYY/month=MM/day=DD/
/raw/csv/installments_payments/year=YYYY/month=MM/day=DD/
/raw/csv/credit_card_balance/year=YYYY/month=MM/day=DD/
/raw/csv/pos_cash_balance/year=YYYY/month=MM/day=DD/
/raw/csv/previous_application/year=YYYY/month=MM/day=DD/
3.3 Ingestion jobs (what each script does)
feeder_postgres.py (Source A → Bronze)
Input
•	PostgreSQL table (JDBC)
Processing
•	minimal: add ingestion date columns
•	do NOT transform business fields
Output
•	write to HDFS as Parquet (recommended)
•	partitioned by year/month/day
Example output dataset
•	bronze_postgres_application_train (as a Hive external table)
________________________________________
feeder_csv.py (Source B → Bronze)
Input
•	local mounted csv folder
Processing
•	minimal: add ingestion date columns
•	schema inference allowed (raw)
Output
•	write to HDFS (Parquet recommended for performance)
•	partitioned by year/month/day
________________________________________
3.4 Exam-critical requirements checklist (Bronze)
•	Executed via spark-submit
•	Paths are parameters (NO hardcoded paths)
•	Logging:
o	log.info("...")
o	log.error("...")
o	logs exported to .txt
•	Partitioning date folders exist and are visible
3.5 Bronze Hive tables (what to register)
Create databases:
•	bronze
Register external tables pointing to HDFS paths, e.g.:
•	bronze.application_train_raw
•	bronze.bureau_raw
•	etc.
Outcome
•	Teacher can query raw data through Hive/Spark SQL
•	Storage is structured and re-runnable
________________________________________
4. Silver layer — Curated & unified data (core grading section)
4.1 Purpose (why Silver is the “points layer”)
This is where you demonstrate:
•	cleaning
•	validation rules (≥5)
•	joins
•	aggregations
•	window functions
•	cache/persist
4.2 Silver storage & naming
HDFS partitioning (mandatory):
/silver/<dataset>/year=YYYY/month=MM/day=DD/
Hive database:
•	silver
Format:
•	Parquet (required by the brief)
________________________________________
4.3 Silver datasets (exact design)
Silver table 1 — silver_client_application
Grain
•	1 row per SK_ID_CURR
Input
•	Bronze from PostgreSQL:
o	bronze.application_train_raw
o	bronze.application_test_raw
Key
•	SK_ID_CURR
Core transformations
•	Cast key numeric columns (income, credit, annuity)
•	Normalize “days” fields to consistent numeric
•	Handle nulls / missing categories
•	Standardize labels where needed (optional)
Validation rules (minimum 5, concrete)
Examples you can implement and log counts for:
1.	SK_ID_CURR is not null
2.	AMT_INCOME_TOTAL > 0
3.	AMT_CREDIT > 0
4.	Applicant age: abs(DAYS_BIRTH)/365 >= 18
5.	AMT_ANNUITY is null OR AMT_ANNUITY > 0
6.	AMT_CREDIT >= AMT_ANNUITY (optional but good)
7.	CODE_GENDER in ('M','F','XNA') (dataset-dependent)
Output
•	/silver/silver_client_application/year=...
•	Hive table silver.silver_client_application
________________________________________
Silver table 2 — silver_bureau_summary
Goal
Turn bureau data into client-level features.
Input
•	bronze.bureau_raw
•	bronze.bureau_balance_raw
Join
•	bureau_balance joins bureau via SK_ID_BUREAU
Window function requirement (clear and defensible)
Example:
•	For each SK_ID_BUREAU, order by MONTHS_BALANCE and select latest status:
o	row_number() over (partition by SK_ID_BUREAU order by MONTHS_BALANCE desc)
Aggregations (per SK_ID_CURR)
•	count of bureau credits
•	count active credits (CREDIT_ACTIVE)
•	total debt sum
•	max days overdue
•	sum overdue amount
Output
•	silver.silver_bureau_summary
________________________________________
Silver table 3 — silver_payment_behavior
Goal
Compute behavioral payment indicators.
Inputs
•	bronze.installments_payments_raw
•	plus optional: credit_card_balance, pos_cash_balance
Key metric
•	delay = DAYS_ENTRY_PAYMENT - DAYS_INSTALMENT
o	positive = late
o	negative = early
Window function (client timeline)
•	e.g., per client order payments by entry date and compute trend, or select last N months:
o	partition by SK_ID_CURR order by DAYS_ENTRY_PAYMENT
Aggregations per client
•	avg delay days
•	count late payments (delay > 0)
•	total paid amount
•	payment ratio = total_paid / total_installment
Output
•	silver.silver_payment_behavior
________________________________________
Silver table 4 — silver_previous_applications
Goal
Summarize client history of applying.
Input
•	bronze.previous_application_raw
Aggregations per client
•	number of previous applications
•	rejection rate = rejected / total
•	avg requested amount vs avg granted
•	distribution by contract status
Output
•	silver.silver_previous_applications
________________________________________
4.4 Performance requirements (must be visible)
You must demonstrate:
•	cache() or persist()
•	visible in Spark UI storage tab
Where to apply it:
•	after expensive joins or large reads (bureau + bureau_balance)
•	before repeated aggregations
Also log:
•	row counts before/after
•	validation failures count
________________________________________
5. Gold layer — Business & risk analytics (decision-ready)
5.1 Purpose
Gold answers the business question directly:
•	Who is risky?
•	What is portfolio exposure by segment?
Gold tables are smaller and more stable.
Hive database:
•	gold (optional as Hive)
Stored as Parquet or materialized directly into datamarts.
________________________________________
5.2 Gold table 1 — gold_client_risk_profile
Grain
•	1 row per SK_ID_CURR
Inputs
•	silver_client_application
•	silver_bureau_summary
•	silver_payment_behavior
•	silver_previous_applications
Join keys
•	SK_ID_CURR
Core business metrics
•	income (from application)
•	credit_exposure (AMT_CREDIT)
•	bureau_debt_ratio = bureau_debt / credit (avoid divide by zero)
•	payment_delay_score from avg delay and late count
•	previous_rejection_rate
•	default_flag (TARGET from train; null for test)
Risk segment logic (rule-based, exam-friendly)
Example:
•	HIGH if (debt_ratio high OR late_payments high OR rejection_rate high)
•	MEDIUM else
•	LOW else
This is easy to explain and doesn’t require ML.
________________________________________
5.3 Gold table 2 — gold_portfolio_risk
Grain
•	1 row per risk_segment
Metrics
•	number of clients
•	total exposure (sum credit)
•	average default rate (avg TARGET for train)
•	avg income per segment (optional)
________________________________________
6. Datamarts (PostgreSQL serving layer)
6.1 Purpose
•	fast SQL for API and dashboards
•	relational structure
•	stable outputs
6.2 Where to store
Use a separate schema in the same PostgreSQL instance, e.g.:
•	datamart
6.3 Tables
•	datamart.datamart_client_risk (client-level)
•	datamart.datamart_portfolio_summary (segment-level)
6.4 Load method
Spark writes via JDBC:
•	.write.format("jdbc")...mode("overwrite")
Outcome:
•	datamarts always reflect latest Gold run
________________________________________
7. API & Visualization
7.1 API requirements (exam)
•	REST API
•	JWT auth
•	pagination mandatory
•	endpoint listing a datamart
Minimal endpoint
GET /clients/risk?page=1&page_size=50
Response returns rows from:
•	datamart_client_risk
7.2 Visualization requirements
Minimum 3 charts from datamarts:
1.	risk distribution (count per segment)
2.	default rate per segment (avg TARGET)
3.	credit exposure per segment (sum AMT_CREDIT)
You can do this with Streamlit or any lightweight frontend.
________________________________________
8. Logging, monitoring & delivery
8.1 Logs
For every Spark script:
•	log.info start/end
•	log.info row counts
•	log.error exceptions
•	output logs to .txt file per run
8.2 Monitoring evidence (video requirements)
You must show:
•	spark-submit execution
•	HDFS raw/silver folders
•	Hive tables
•	Spark UI (cache/persist visible)
•	Resource Manager (YARN bonus)
•	PostgreSQL datamarts
•	API endpoint working
•	dashboard charts
________________________________________
9. Execution order (strict)
1.	PostgreSQL source ✅ done
2.	Implement Bronze ingestion scripts (feeder_postgres.py, feeder_csv.py)
3.	Write Bronze to HDFS with ingestion partitions
4.	Create Silver processor (processor.py) with:
o	5 validation rules
o	join
o	aggregation
o	window function
o	cache/persist
5.	Compute Gold KPIs
6.	Load PostgreSQL datamarts (datamart.py)
7.	Build API (JWT + pagination)
8.	Build visualization (3 charts)
9.	Record demo video (5–8 min)

