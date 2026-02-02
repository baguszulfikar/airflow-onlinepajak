# Student Data Pipeline

An end-to-end data pipeline that ingests raw student data from multiple sources and produces analytics-ready datasets for student and class performance reporting.

## Table of Contents
- [Architecture Overview](#architecture-overview)
- [Data Sources](#data-sources)
- [Ingestion Strategy](#ingestion-strategy)
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
│                            INGESTION LAYER (BRONZE)                             │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐                  │
│  │ CSV Ingestion   │  │ CSV Ingestion   │  │ JSON Ingestion  │                  │
│  │ - Schema valid  │  │ - Schema valid  │  │ - Parse & flat  │                  │
│  │ - Type casting  │  │ - Type casting  │  │ - Type casting  │                  │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘                  │
│           │                    │                    │                           │
│           ▼                    ▼                    ▼                           │
│  ┌─────────────────────────────────────────────────────────────────┐            │
│  │                    BRONZE TABLES (Raw + Metadata)               │            │
│  │  • bronze_students    • bronze_attendance    • bronze_assessments│           │
│  └─────────────────────────────────────────────────────────────────┘            │
└─────────────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          TRANSFORMATION LAYER (SILVER)                          │
│  ┌─────────────────────────────────────────────────────────────────┐            │
│  │                      DATA QUALITY & CLEANING                    │            │
│  │  • Deduplication      • Null handling      • Data validation    │            │
│  │  • Standardization    • Referential integrity checks            │            │
│  └─────────────────────────────────────────────────────────────────┘            │
│           │                    │                    │                           │
│           ▼                    ▼                    ▼                           │
│  ┌─────────────────────────────────────────────────────────────────┐            │
│  │                    SILVER TABLES (Cleaned)                      │            │
│  │  • dim_students       • fact_attendance      • fact_assessments │            │
│  └─────────────────────────────────────────────────────────────────┘            │
└─────────────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            ANALYTICS LAYER (GOLD)                               │
│  ┌─────────────────────────────────────────────────────────────────┐            │
│  │                      AGGREGATION & METRICS                      │            │
│  │  • Join dimensions    • Calculate KPIs      • Build aggregates  │            │
│  └─────────────────────────────────────────────────────────────────┘            │
│           │                    │                    │                           │
│           ▼                    ▼                    ▼                           │
│  ┌─────────────────────────────────────────────────────────────────┐            │
│  │                    GOLD TABLES (Analytics-Ready)                │            │
│  │  • class_daily_performance    • student_performance_summary     │            │
│  └─────────────────────────────────────────────────────────────────┘            │
└─────────────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              ORCHESTRATION                                      │
│  ┌─────────────────────────────────────────────────────────────────┐            │
│  │                        APACHE AIRFLOW                           │            │
│  │  • DAG: student_data_pipeline                                   │            │
│  │  • Schedule: Daily at 6:00 AM                                   │            │
│  │  • Tasks: ingest → transform → aggregate                        │            │
│  └─────────────────────────────────────────────────────────────────┘            │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Why This Architecture?

1. **Medallion Architecture (Bronze → Silver → Gold)**
   - **Bronze Layer**: Raw data ingestion with minimal transformation. Preserves original data for audit trails and reprocessing.
   - **Silver Layer**: Cleaned, deduplicated, and validated data. Business rules applied here.
   - **Gold Layer**: Aggregated, analytics-ready tables optimized for reporting.

2. **Separation of Concerns**
   - Each layer has a single responsibility
   - Easy to debug and maintain
   - Supports incremental processing

3. **DuckDB as Data Warehouse**
   - Lightweight, no external dependencies
   - SQL-based analytics
   - Fast columnar storage
   - Easy to deploy and scale

---

## Data Sources

| Source | Format | Description | Update Frequency |
|--------|--------|-------------|------------------|
| students.csv | CSV | Student master data (ID, name, class, grade) | Daily |
| attendance.csv | CSV | Daily attendance records | Real-time/Daily |
| assessments.json | JSON | Assessment scores and results | Event-driven |

---

## Ingestion Strategy

### 1. Students Data (CSV)
```
Strategy: Full Load with Upsert
- File format: CSV with header
- Primary key: student_id
- Approach: Load entire file, upsert based on student_id
- Validation: Check for required fields, valid class_id format
- Frequency: Daily
```

**Why Full Load?**
- Student data is relatively small (<10K records typically)
- Need to capture status changes (enrollment_status)
- Simple to implement and maintain

### 2. Attendance Data (CSV)
```
Strategy: Incremental Load with Append
- File format: CSV with header
- Primary key: attendance_id
- Approach: Append new records, deduplicate by attendance_id
- Validation: Valid student_id reference, valid date format
- Frequency: Daily
```

**Why Incremental?**
- Attendance records are append-only (historical data doesn't change)
- Volume grows daily
- Efficient processing

### 3. Assessments Data (JSON)
```
Strategy: Incremental Load with Append
- File format: JSON array
- Primary key: assessment_id
- Approach: Parse JSON, flatten, append new records
- Validation: Score within valid range, valid student_id
- Frequency: Event-driven / Daily batch
```

**Why JSON Handling?**
- Flexible schema for different assessment types
- Easy to extend with nested data
- Common format for API responses

---

## Data Model

### Bronze Layer (Raw)
```sql
-- bronze_students
student_id VARCHAR PRIMARY KEY,
student_name VARCHAR,
class_id VARCHAR,
grade_level INTEGER,
enrollment_status VARCHAR,
updated_at TIMESTAMP,
_ingested_at TIMESTAMP,
_source_file VARCHAR

-- bronze_attendance
attendance_id VARCHAR PRIMARY KEY,
student_id VARCHAR,
attendance_date DATE,
status VARCHAR,
created_at TIMESTAMP,
_ingested_at TIMESTAMP,
_source_file VARCHAR

-- bronze_assessments
assessment_id VARCHAR PRIMARY KEY,
student_id VARCHAR,
subject VARCHAR,
score INTEGER,
max_score INTEGER,
assessment_date DATE,
created_at TIMESTAMP,
_ingested_at TIMESTAMP,
_source_file VARCHAR
```

### Silver Layer (Cleaned)
```sql
-- dim_students (Dimension Table)
student_id VARCHAR PRIMARY KEY,
student_name VARCHAR NOT NULL,
class_id VARCHAR NOT NULL,
grade_level INTEGER NOT NULL,
enrollment_status VARCHAR NOT NULL,
is_active BOOLEAN,
updated_at TIMESTAMP

-- fact_attendance (Fact Table)
attendance_id VARCHAR PRIMARY KEY,
student_id VARCHAR REFERENCES dim_students,
class_id VARCHAR,
attendance_date DATE NOT NULL,
status VARCHAR NOT NULL,
is_present BOOLEAN

-- fact_assessments (Fact Table)
assessment_id VARCHAR PRIMARY KEY,
student_id VARCHAR REFERENCES dim_students,
class_id VARCHAR,
subject VARCHAR NOT NULL,
score INTEGER NOT NULL,
max_score INTEGER NOT NULL,
score_percentage DECIMAL(5,2),
assessment_date DATE NOT NULL
```

### Gold Layer (Analytics)
```sql
-- class_daily_performance
class_id VARCHAR,
date DATE,
total_students INTEGER,
students_with_attendance INTEGER,
present_count INTEGER,
absent_count INTEGER,
attendance_rate DECIMAL(5,2),
students_with_assessment INTEGER,
assessment_count INTEGER,
avg_score DECIMAL(5,2),
PRIMARY KEY (class_id, date)
```

---

## How to Run

### Prerequisites
- Python 3.9+
- pip

### Option 1: Run Locally

```bash
# 1. Clone and setup
cd student-data-pipeline
pip install -r requirements.txt

# 2. Copy raw data to data/raw folder
cp ../Sampel\ data\ Test\ DE-*/*.csv data/raw/
cp ../Sampel\ data\ Test\ DE-*/*.json data/raw/

# 3. Run the pipeline
python src/main.py

# 4. Query results
python src/query_results.py
```

### Option 2: Run with Docker

```bash
# Build and run
docker-compose up --build

# Or run pipeline only
docker build -t student-pipeline .
docker run -v $(pwd)/data:/app/data student-pipeline
```

### Option 3: Run with Airflow

```bash
# Start Airflow
docker-compose -f docker/docker-compose-airflow.yml up -d

# Access Airflow UI at http://localhost:8080
# Username: airflow, Password: airflow

# Trigger DAG manually or wait for scheduled run
```

---

## Automation

### Airflow DAG Structure

```
student_data_pipeline (Daily @ 6:00 AM)
│
├── ingest_students ──────┐
├── ingest_attendance ────┼──► transform_data ──► build_analytics ──► validate_output
└── ingest_assessments ───┘
```

### Scheduling Options

| Method | Use Case | Configuration |
|--------|----------|---------------|
| Airflow | Production, complex workflows | `dags/student_pipeline_dag.py` |
| Cron | Simple scheduling | `0 6 * * * python src/main.py` |
| Manual | Development, testing | `python src/main.py` |

---

## Project Structure

```
student-data-pipeline/
├── src/
│   ├── ingestion/          # Data ingestion modules
│   │   ├── __init__.py
│   │   ├── csv_ingestion.py
│   │   └── json_ingestion.py
│   ├── transformation/     # Data cleaning & transformation
│   │   ├── __init__.py
│   │   └── transform.py
│   ├── analytics/          # Gold layer aggregations
│   │   ├── __init__.py
│   │   └── aggregations.py
│   ├── utils/              # Shared utilities
│   │   ├── __init__.py
│   │   └── db.py
│   ├── main.py             # Pipeline entry point
│   └── query_results.py    # Query helper
├── dags/
│   └── student_pipeline_dag.py
├── data/
│   ├── raw/                # Source files
│   ├── bronze/             # Raw ingested data
│   ├── silver/             # Cleaned data
│   └── gold/               # Analytics tables
├── docker/
│   └── docker-compose-airflow.yml
├── tests/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Output

After running the pipeline, you can query the analytics tables:

```sql
-- Class Daily Performance
SELECT * FROM class_daily_performance
WHERE class_id = 'CLASS-01'
ORDER BY date;

-- Example Output:
-- class_id  | date       | total_students | students_with_attendance | present_count | ...
-- CLASS-01  | 2025-01-10 | 30             | 28                       | 25            | ...
```
