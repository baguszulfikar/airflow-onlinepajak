# Student Data Pipeline

An end-to-end data pipeline that ingests raw student data from multiple sources and produces analytics-ready datasets for student and class performance reporting.

## Table of Contents
- [Architecture Overview](#architecture-overview)
- [Project Structure](#project-structure)
- [Data Sources](#data-sources)
- [Data Model](#data-model)
- [How to Run](#how-to-run)
- [Automation](#automation)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         STUDENT DATA PIPELINE ARCHITECTURE                       │
└─────────────────────────────────────────────────────────────────────────────────┘

┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│   DATA SOURCES   │     │   DATA SOURCES   │     │   DATA SOURCES   │
│                  │     │                  │     │                  │
│  students.csv    │     │  attendance.csv  │     │ assessments.json │
│  (CSV Format)    │     │  (CSV Format)    │     │  (JSON Format)   │
└────────┬─────────┘     └────────┬─────────┘     └────────┬─────────┘
         │                        │                        │
         └────────────────────────┼────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      INGESTION LAYER (Python + Airflow)                         │
│  ┌─────────────────────────────────────────────────────────────────┐            │
│  │                    BRONZE TABLES (Raw + Metadata)               │            │
│  │  • bronze.students    • bronze.attendance    • bronze.assessments│           │
│  └─────────────────────────────────────────────────────────────────┘            │
│                           DAG: student_ingestion                                │
└─────────────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼ triggers
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        TRANSFORMATION LAYER (dbt)                               │
│  ┌─────────────────────────────────────────────────────────────────┐            │
│  │                    SILVER TABLES (Cleaned)                      │            │
│  │  • silver.dim_students  • silver.fact_attendance                │            │
│  │  • silver.fact_assessments                                      │            │
│  └─────────────────────────────────────────────────────────────────┘            │
│                                  │                                              │
│                                  ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────┐            │
│  │                    GOLD TABLES (Analytics-Ready)                │            │
│  │  • gold.class_daily_performance                                 │            │
│  │  • gold.student_performance_summary                             │            │
│  └─────────────────────────────────────────────────────────────────┘            │
│                           DAG: student_dbt                                      │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Why This Architecture?

1. **Medallion Architecture (Bronze → Silver → Gold)**
   - **Bronze Layer**: Raw data ingestion with minimal transformation (Python)
   - **Silver Layer**: Cleaned, deduplicated, and validated data (dbt)
   - **Gold Layer**: Aggregated, analytics-ready tables (dbt)

2. **Separation of Concerns**
   - **Ingestion**: Python handles source file parsing and Bronze loading
   - **Transformation**: dbt handles all SQL transformations (separate repo)
   - **Orchestration**: Airflow coordinates the workflow

3. **DuckDB as Data Warehouse**
   - Lightweight, no external dependencies
   - SQL-based analytics
   - Fast columnar storage

---

## Project Structure

```
student-data-pipeline/              # This repo - Ingestion
├── src/
│   ├── ingestion/                  # Data ingestion modules
│   │   ├── csv_ingestion.py
│   │   └── json_ingestion.py
│   ├── utils/
│   │   └── db.py                   # Database utilities
│   ├── main.py                     # Pipeline entry point
│   └── query_results.py            # Query helper
├── dags/
│   ├── student_ingestion_dag.py    # Ingestion DAG
│   └── student_dbt_dag.py          # dbt DAG
├── data/
│   └── raw/                        # Source files
├── docker/
│   └── docker-compose-airflow.yml
├── Dockerfile
└── requirements.txt

student-analytics-dbt/              # Separate repo - Transformations
├── models/
│   ├── sources.yml                 # Bronze sources
│   ├── silver/                     # Silver layer models
│   │   ├── dim_students.sql
│   │   ├── fact_attendance.sql
│   │   └── fact_assessments.sql
│   └── gold/                       # Gold layer models
│       ├── class_daily_performance.sql
│       └── student_performance_summary.sql
├── dbt_project.yml
└── profiles.yml
```

---

## Data Sources

| Source | Format | Description | Update Frequency |
|--------|--------|-------------|------------------|
| students.csv | CSV | Student master data (ID, name, class, grade) | Daily |
| attendance.csv | CSV | Daily attendance records | Real-time/Daily |
| assessments.json | JSON | Assessment scores and results | Event-driven |

---

## Data Model

### Bronze Layer (Raw - Python Ingestion)
```sql
bronze.students      -- Raw student data with metadata
bronze.attendance    -- Raw attendance records
bronze.assessments   -- Raw assessment data
```

### Silver Layer (Cleaned - dbt)
```sql
silver.dim_students      -- Deduplicated student dimension
silver.fact_attendance   -- Attendance facts with class enrichment
silver.fact_assessments  -- Assessment facts with score percentage
```

### Gold Layer (Analytics - dbt)
```sql
gold.class_daily_performance      -- Daily class metrics
gold.student_performance_summary  -- Student performance summary
```

---

## How to Run

### Prerequisites
- Docker & Docker Compose
- Python 3.11+

### Quick Start with Airflow

```bash
# 1. Start Airflow
cd docker
docker-compose -f docker-compose-airflow.yml up -d

# 2. Access Airflow UI
# URL: http://localhost:8080
# Username: airflow
# Password: airflow

# 3. Enable and trigger the DAGs
# - student_ingestion (runs daily at 6AM, triggers student_dbt)
# - student_dbt (triggered by ingestion DAG)
```

### Run Locally (Ingestion Only)

```bash
# Install dependencies
pip install -r requirements.txt

# Copy raw data
cp /path/to/data/*.csv data/raw/
cp /path/to/data/*.json data/raw/

# Run ingestion
python src/main.py

# Run dbt (from dbt project)
cd ../student-analytics-dbt
dbt run --profiles-dir .
```

---

## Automation

### Airflow DAGs

**1. student_ingestion** (Scheduled: Daily @ 6:00 AM)
```
start → init_db → ingest_students → ingest_attendance → ingest_assessments → trigger_dbt → end
```

**2. student_dbt** (Triggered by ingestion DAG)
```
start → dbt_deps → dbt_run_silver → dbt_test_silver → dbt_run_gold → dbt_test_gold → validate → end
```

### Data Flow

```
┌─────────────────────┐         ┌─────────────────────┐
│  student_ingestion  │         │     student_dbt     │
│   (Daily 6:00 AM)   │         │    (Triggered)      │
├─────────────────────┤         ├─────────────────────┤
│ • init_database     │         │ • dbt_deps          │
│ • ingest_students   │         │ • dbt_run_silver    │
│ • ingest_attendance │────────►│ • dbt_test_silver   │
│ • ingest_assessments│ trigger │ • dbt_run_gold      │
│ • trigger_dbt_dag   │         │ • dbt_test_gold     │
└─────────────────────┘         │ • validate_output   │
                                └─────────────────────┘
```

---

## Output

Query the analytics tables:

```sql
-- Class Daily Performance
SELECT * FROM gold.class_daily_performance
WHERE class_id = 'CLASS-01'
ORDER BY date;

-- Student Performance Summary
SELECT * FROM gold.student_performance_summary
WHERE attendance_rate < 0.8
ORDER BY avg_score DESC;
```
