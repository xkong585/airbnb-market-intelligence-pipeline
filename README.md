# Airbnb Data Pipeline

## Project participation

This was a collaborative group project, and I actively contributed to every stage of its development.

Original team repository: [CocoChengtw/airbnb-market-intelligence-pipeline](https://github.com/CocoChengtw/airbnb-market-intelligence-pipeline).

This repository contains a project snapshot for my portfolio. Original project documentation and acknowledgments are retained below.

## Project Overview

Short-term rental platforms such as Airbnb have significantly reshaped urban tourism and local housing markets. Understanding how Airbnb activity varies across neighborhoods can provide insights for real estate investors, property managers, urban planners, and policymakers.

This project builds a scalable data engineering pipeline that transforms raw Airbnb listing data into structured neighborhood-level market indicators across multiple U.S. cities.

The pipeline integrates:
- Airbnb listings data
- Review activity data
- Socioeconomic information from the U.S. Census (ACS median household income)

The final system produces curated analytical tables and an interactive dashboard that allow users to explore:
- Airbnb supply patterns
- Demand signals
- Host market structure
- Neighborhood socioeconomic context

The project demonstrates how large-scale marketplace data can be transformed into decision-ready insights using modern data engineering tools.

---

## Pipeline

A three-layer **Bronze → Silver → Gold** ETL pipeline for Airbnb listing data. Uses Apache Spark and Apache Sedona for geospatial processing, orchestrated by Apache Airflow on a GCP VM, with final output loaded into Snowflake.

---

## Project Structure

```
final_project/
├── scripts/
│   ├── silver_pipeline.py        # Bronze → Silver (Spark + Sedona spatial join)
│   ├── gold_pipeline.py          # Silver → Gold (aggregated analytical tables)
│   └── snowflake_ingest.py       # Gold → Snowflake (pandas write_pandas)
├── dags/
│   └── airbnb_pipeline_dag.py    # Airflow DAG (chains the three jobs)
├── snowflake_setup.sql           # Snowflake DDL setup and data quality checks
├── mart_tables.sql               # Mart views built on top of gold tables for visualization
├── bronze2silver_pipeline.ipynb  # Original development notebook (kept for reference)
├── reddit_collector.py           # Reddit keyword trend collector (standalone script)
└── setup_vm.sh                   # One-time GCP VM setup script
```

---

## Data Architecture

```
/shared/data/
├── bronze/                              # Raw data (read-only)
│   ├── city=los_angeles/
│   │   ├── snapshot_date=2025-03-01/listings.csv.gz
│   │   ├── snapshot_date=2025-06-17/listings.csv.gz
│   │   ├── snapshot_date=2025-09-01/listings.csv.gz
│   │   ├── snapshot_date=2025-12-04/listings.csv.gz
│   │   ├── shape_ca_2024/               # Census tract shapefiles (2024)
│   │   ├── shape_ca_2025/               # Census tract shapefiles (2025)
│   │   └── income_la/                   # ACS B19013 median income data
│   ├── city=new_york_city/
│   │   ├── snapshot_date=2025-03-01/listings.csv.gz
│   │   ├── snapshot_date=2025-06-17/listings.csv.gz
│   │   ├── snapshot_date=2025-09-01/listings.csv.gz
│   │   ├── snapshot_date=2025-12-04/listings.csv.gz
│   │   ├── shape_nyc_2024/
│   │   ├── shape_nyc_2025/
│   │   └── income_nyc/
│   ├── city=portland/
│   │   ├── snapshot_date=2025-03-03/listings.csv.gz
│   │   ├── snapshot_date=2025-06-17/listings.csv.gz
│   │   ├── snapshot_date=2025-09-06/listings.csv.gz
│   │   ├── snapshot_date=2025-12-04/listings.csv.gz
│   │   ├── shape_oregon_2024/
│   │   ├── shape_oregon_2025/
│   │   └── income_oregon/
│   └── city=san_francisco/
│       ├── snapshot_date=2025-03-01/listings.csv.gz
│       ├── snapshot_date=2025-06-17/listings.csv.gz
│       ├── snapshot_date=2025-09-01/listings.csv.gz
│       ├── snapshot_date=2025-12-04/listings.csv.gz
│       └── income_sf/                   # Uses shape_ca_2025 from city=los_angeles/
├── silver/
│   └── listings_enriched/               # Cleaned + geo-enriched + income-joined (parquet)
│       └── city=*/snapshot_date=*/
└── golden/
    ├── fact_neighborhood_month/         # Neighborhood-level monthly stats
    ├── fact_host_summary/               # Host-level summary per neighborhood
    └── fact_listing_summary/            # Individual listing metrics
```

### Cities and Snapshots

| City | Snapshot Dates |
|------|---------------|
| Los Angeles | 2025-03-01, 2025-06-17, 2025-09-01, 2025-12-04 |
| New York City | 2025-03-01, 2025-06-17, 2025-09-01, 2025-12-04 |
| Portland | 2025-03-03, 2025-06-17, 2025-09-06, 2025-12-04 |
| San Francisco | 2025-03-01, 2025-06-17, 2025-09-01, 2025-12-04 |

---

## Pipeline Overview

```
silver  ──────────────►  gold  ──────────────►  snowflake_ingest
  Spark + Sedona           PySpark                Python
  CSV → spatial join       Read silver →          Read gold parquet
  + income join            → 3 gold tables        → write_pandas to
  ~30–60 min               ~10 min                  Snowflake ~5 min
```

### Task 1 — `silver`

1. Read `listings.csv.gz` for each city and snapshot date from `/shared/data/bronze`
2. Clean coordinates (cast to double, filter invalid values)
3. Spatial join with Census tract shapefiles using **Apache Sedona** `ST_Contains`
4. Join ACS B19013 median household income by tract GEOID
5. Write partitioned parquet to `/shared/data/silver/listings_enriched` (partitioned by `city` / `snapshot_date`)

### Task 2 — `gold`

Produces three analytical tables from the silver layer:

| Gold Table | Description |
|-----------|-------------|
| `fact_neighborhood_month` | Listing count, review count, avg occupancy, and avg income per neighborhood per month |
| `fact_host_summary` | Host count and avg listings per host per neighborhood |
| `fact_listing_summary` | Full metrics per individual listing (room type, reviews, revenue, etc.) |

### Task 3 — `snowflake_ingest`

Loads the three gold parquet tables into Snowflake using `write_pandas`:

| Snowflake Table | Source |
|----------------|--------|
| `FACT_NEIGHBORHOOD_MONTH` | `/shared/data/golden/fact_neighborhood_month` |
| `FACT_HOST_SUMMARY` | `/shared/data/golden/fact_host_summary` |
| `FACT_LISTING_SUMMARY` | `/shared/data/golden/fact_listing_summary` |

---

## Mart Tables

[mart_tables.sql](mart_tables.sql) builds aggregated views on top of the gold tables for use in downstream visualization and business analysis. All views operate at the neighborhood or neighborhood/quarter grain.

| View | Description |
|------|-------------|
| `vw_market_health` | Neighborhood-level occupancy, availability, reviews, and income with Low/Mid/High income tier segmentation |
| `vw_price_supply_trends` | Supply and revenue trends by neighborhood, room type, and month — includes month-over-month listing growth rate |
| `vw_roomtype_by_income` | Room type breakdown by neighborhood with occupancy, revenue, and income group |
| `vw_income_tier_comparison` | Aggregates listings, reviews, and occupancy by income tier (Low/Mid/High) per city per month |
| `vw_host_concentration` | Host market concentration per neighborhood — flags high/medium/low concentration risk based on largest host's market share |
| `vw_saturation_risk` | Identifies oversupplied neighborhoods using latest snapshot — flags High/Medium/Low saturation risk based on listing count vs. demand signals |

---

## Snowflake

Access the Snowflake workspace: [app.snowflake.com](https://app.snowflake.com/us-east-1/dxc27173/#/workspaces/ws/USER%24/PUBLIC/DEFAULT%24/Untitled.sql)

After the pipeline runs, the three gold tables are available under `AIRBNB_PIPELINE.GOLD`.

### Snowflake Setup

[snowflake_setup.sql](snowflake_setup.sql) contains the DDL and data quality queries used to set up the Snowflake environment:

- Database, schema, and warehouse creation
- Combined cross-city tables (`FACT_NEIGHBORHOOD_ALL`, `FACT_LISTING_ALL`)
- Data quality checks (null rates, duplicate checks, row counts per city)

---

## Requirements

| Package | Version |
|---------|---------|
| Python | 3.10+ |
| Apache Spark | 3.5.x |
| Apache Sedona | 1.8.1 |
| Apache Airflow | 2.9.x |
| Java | 17 |
| snowflake-connector-python | latest |

---

## GCP VM Deployment

### 1. Run the setup script (one-time only)

```bash
cd /shared
bash final_project/setup_vm.sh
```

The script handles: Java 17 installation, Python package installation, Airflow initialization, admin account creation, setting `dags_folder` to `/shared/final_project/dags`, and starting the scheduler and webserver.

### 2. Open firewall for the Airflow UI (one-time only)

If port 8080 is not yet open, run:

```bash
gcloud compute firewall-rules create allow-airflow \
  --allow tcp:8080 \
  --target-tags airflow-vm
```

If the rule already exists, skip this step.

### 3. Start Airflow

```bash
airflow scheduler &
airflow webserver --port 8080 &
```

### 4. Verify Airflow is running

Open `http://34.94.143.232:8080` in your browser. Login with username `admin` and the password set in `config.env`.

> If accessing via VSCode SSH, port 8080 is auto-forwarded — use `http://localhost:8080` instead.

---

## Running the Pipeline

```bash
# Verify the DAG is loaded
airflow dags list | grep airbnb

# Trigger the pipeline
airflow dags trigger airbnb_pipeline
```

---

## Dashboard

The final interactive dashboard is built in Tableau and published publicly:

[View Dashboard on Tableau Public](https://public.tableau.com/views/Version1_17736010325130/FINALDASHBOARD?:language=en-US&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link)

---

## Data Access

The raw and processed data resides on the GCP VM shared disk (`/shared/data/`). If you need access to the data, please email us at **xiangyi.kong.2026@anderson.ucla.edu** with your Google account and we will grant you read access to the GCP project.

---

## Schedule Configuration

Edit `SCHEDULE` in [dags/airbnb_pipeline_dag.py](dags/airbnb_pipeline_dag.py):

```python
SCHEDULE = None          # Manual trigger only (default)
```
