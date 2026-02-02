# Student Data Pipeline

An end-to-end data pipeline that ingests raw student data from multiple sources and produces analytics-ready datasets for student and class performance reporting.

## Table of Contents
- [Architecture Overview](#architecture-overview)
- [Architecture Design Decisions](#architecture-design-decisions)
- [Ingestion Strategy](#ingestion-strategy)
- [Project Structure](#project-structure)
- [Data Model](#data-model)
- [How to Run](#how-to-run)
- [View Analytics](#view-analytics)
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

---

## Architecture Design Decisions

### 1. Why Medallion Architecture (Bronze → Silver → Gold)?

| Layer | Purpose | Technology | Rationale |
|-------|---------|------------|-----------|
| **Bronze** | Raw data landing zone | Python + Pandas | Preserves original data exactly as received for audit trails and reprocessing |
| **Silver** | Cleaned & validated data | dbt | Business rules, deduplication, and data quality checks in version-controlled SQL |
| **Gold** | Analytics-ready aggregates | dbt | Pre-computed metrics optimized for reporting and dashboards |

**Benefits:**
- **Reprocessing**: If transformation logic changes, we can reprocess from Bronze without re-ingestion
- **Debugging**: Easy to trace data issues back to the source
- **Flexibility**: Each layer can evolve independently

### 2. Why Separate Ingestion (Python) from Transformation (dbt)?

| Concern | Technology | Reason |
|---------|------------|--------|
| **File Parsing** | Python | Better handling of various file formats (CSV, JSON, APIs) |
| **Data Transformation** | dbt | SQL-based transformations are more readable, testable, and maintainable |
| **Orchestration** | Airflow | Handles dependencies, retries, and scheduling |

**Benefits:**
- **Separation of Concerns**: Ingestion team can work independently from analytics team
- **Testability**: dbt provides built-in testing framework for data quality
- **Documentation**: dbt auto-generates documentation and lineage graphs
- **Version Control**: SQL transformations are easier to review in PRs

### 3. Why Two Separate DAGs?

```
┌─────────────────────┐         ┌─────────────────────┐
│  student_ingestion  │         │     student_dbt     │
│   (Scheduled)       │────────►│    (Triggered)      │
└─────────────────────┘         └─────────────────────┘
```

**Reasons:**
- **Independent Scaling**: Ingestion can run more frequently if needed
- **Failure Isolation**: Ingestion failure doesn't block dbt runs on existing data
- **Flexibility**: dbt DAG can be triggered manually for development/testing
- **Clear Ownership**: Different teams can own different DAGs

### 4. Why DuckDB?

| Feature | Benefit |
|---------|---------|
| **Embedded Database** | No separate server to manage |
| **Columnar Storage** | Fast analytical queries |
| **SQL Compatible** | Works with dbt out of the box |
| **Low Resource** | Runs on minimal infrastructure |

**Trade-offs:**
- Single-writer limitation (handled by sequential task execution)
- Not suitable for high-concurrency production workloads
- For production, consider migrating to PostgreSQL, Snowflake, or BigQuery

### 5. Why Sequential Ingestion Tasks?

```
ingest_students → ingest_attendance → ingest_assessments
```

**Reason:** DuckDB only allows one writer at a time. Running ingestion tasks in parallel would cause lock conflicts. This is a known limitation we accept for the simplicity of using DuckDB.

---

## Ingestion Strategy

### Overview

| Source | Format | Strategy | Primary Key | Frequency |
|--------|--------|----------|-------------|-----------|
| students.csv | CSV | Full Load + Upsert | student_id | Daily |
| attendance.csv | CSV | Append + Dedupe | attendance_id | Daily |
| assessments.json | JSON | Append + Dedupe | assessment_id | Event-driven |

---

### 1. Students Data (`students.csv`)

```
┌─────────────────────────────────────────────────────────────┐
│                    STUDENTS INGESTION                        │
├─────────────────────────────────────────────────────────────┤
│  Strategy:     Full Load with Upsert                        │
│  Primary Key:  student_id                                   │
│  Frequency:    Daily                                        │
│  Volume:       ~1,000-10,000 records                        │
└─────────────────────────────────────────────────────────────┘
```

**Schema:**
```csv
student_id,student_name,class_id,grade_level,enrollment_status,updated_at
STU001,John Doe,CLASS-01,10,ACTIVE,2025-01-15 08:00:00
```

**Strategy: Full Load with Upsert**

| Step | Action | SQL |
|------|--------|-----|
| 1 | Read entire CSV file | `pandas.read_csv()` |
| 2 | Add metadata columns | `_ingested_at`, `_source_file` |
| 3 | Upsert to Bronze | `INSERT OR REPLACE INTO bronze.students` |

**Why Full Load?**
- Student data is relatively small (<10K records typically)
- Need to capture status changes (e.g., `enrollment_status: ACTIVE → INACTIVE`)
- Master data doesn't have reliable "changed since" timestamp
- Simple to implement and guaranteed consistency

**Validations:**
- `student_id` must not be null
- `class_id` format validation (e.g., `CLASS-XX`)
- `grade_level` must be positive integer

---

### 2. Attendance Data (`attendance.csv`)

```
┌─────────────────────────────────────────────────────────────┐
│                   ATTENDANCE INGESTION                       │
├─────────────────────────────────────────────────────────────┤
│  Strategy:     Incremental Append with Deduplication        │
│  Primary Key:  attendance_id                                │
│  Frequency:    Daily                                        │
│  Volume:       ~10,000-50,000 records/day                   │
└─────────────────────────────────────────────────────────────┘
```

**Schema:**
```csv
attendance_id,student_id,attendance_date,status,created_at
ATT001,STU001,2025-01-15,PRESENT,2025-01-15 08:30:00
```

**Strategy: Incremental Append with Deduplication**

| Step | Action | Rationale |
|------|--------|-----------|
| 1 | Read CSV file | Daily attendance export |
| 2 | Add metadata | `_ingested_at`, `_source_file` |
| 3 | Insert with conflict handling | `INSERT OR IGNORE` to skip duplicates |

**Why Incremental Append?**
- Attendance records are immutable (historical data doesn't change)
- Volume grows daily, full load would be inefficient
- `attendance_id` provides natural deduplication key
- Supports late-arriving data (re-processing same file is safe)

**Validations:**
- `attendance_id` must be unique
- `student_id` must exist (validated in Silver layer)
- `status` must be `PRESENT` or `ABSENT`
- `attendance_date` must be valid date

**Handling Duplicates:**
```sql
-- Bronze layer: Insert or ignore duplicates
INSERT OR IGNORE INTO bronze.attendance ...

-- Silver layer: Deduplicate by keeping latest ingested record
ROW_NUMBER() OVER (PARTITION BY attendance_id ORDER BY _ingested_at DESC)
```

---

### 3. Assessments Data (`assessments.json`)

```
┌─────────────────────────────────────────────────────────────┐
│                   ASSESSMENTS INGESTION                      │
├─────────────────────────────────────────────────────────────┤
│  Strategy:     JSON Parse + Flatten + Append                │
│  Primary Key:  assessment_id                                │
│  Frequency:    Event-driven / Daily batch                   │
│  Volume:       Variable (~1,000-20,000 records/batch)       │
└─────────────────────────────────────────────────────────────┘
```

**Schema:**
```json
[
  {
    "assessment_id": "ASM001",
    "student_id": "STU001",
    "subject": "Mathematics",
    "score": 85,
    "max_score": 100,
    "assessment_date": "2025-01-15",
    "created_at": "2025-01-15T10:30:00"
  }
]
```

**Strategy: JSON Parse + Flatten + Append**

| Step | Action | Rationale |
|------|--------|-----------|
| 1 | Parse JSON array | Handle nested structures |
| 2 | Flatten to tabular | Convert to DataFrame |
| 3 | Type casting | Ensure correct data types |
| 4 | Append to Bronze | `INSERT OR IGNORE` for idempotency |

**Why JSON Format?**
- Flexible schema for different assessment types
- Common format for API responses
- Supports nested data if needed in future
- Easy to extend with additional fields

**Validations:**
- `assessment_id` must be unique
- `score` must be between 0 and `max_score`
- `max_score` must be positive
- `assessment_date` must be valid date

**Type Casting:**
```python
# Ensure proper types before loading
df['score'] = df['score'].astype(int)
df['max_score'] = df['max_score'].astype(int)
df['assessment_date'] = pd.to_datetime(df['assessment_date']).dt.date
```

---

### Metadata Columns

All Bronze tables include metadata columns for traceability:

| Column | Type | Description |
|--------|------|-------------|
| `_ingested_at` | TIMESTAMP | When the record was ingested |
| `_source_file` | VARCHAR | Original source file path |

**Why Metadata?**
- **Debugging**: Trace issues back to source files
- **Auditing**: Know when data was loaded
- **Reprocessing**: Identify records from specific loads

---

## Project Structure

```
student-data-pipeline/              # This repo - Ingestion
├── src/
│   ├── ingestion/                  # Data ingestion modules
│   │   ├── csv_ingestion.py        # CSV file ingestion (students, attendance)
│   │   └── json_ingestion.py       # JSON file ingestion (assessments)
│   ├── utils/
│   │   └── db.py                   # Database connection utilities
│   ├── main.py                     # Pipeline entry point
│   └── query_results.py            # Query helper for testing
├── dags/
│   ├── student_ingestion_dag.py    # Ingestion DAG (scheduled)
│   └── student_dbt_dag.py          # dbt DAG (triggered)
├── data/
│   └── raw/                        # Source files location
├── docker/
│   └── docker-compose-airflow.yml  # Airflow deployment
├── show_analytics.py               # Analytics dashboard script
├── Dockerfile
└── requirements.txt

student-analytics-dbt/              # Separate repo - Transformations
├── models/
│   ├── sources.yml                 # Bronze sources definition
│   ├── silver/                     # Silver layer models
│   └── gold/                       # Gold layer models
├── dbt_project.yml
└── profiles.yml
```

---

## Data Model

### Bronze Layer (Raw - Python Ingestion)
```sql
bronze.students      -- Raw student master data
bronze.attendance    -- Raw daily attendance records
bronze.assessments   -- Raw assessment scores
```

### Silver Layer (Cleaned - dbt)
```sql
silver.dim_students      -- Deduplicated, with is_active flag
silver.fact_attendance   -- Enriched with class_id, is_present flag
silver.fact_assessments  -- With calculated score_percentage
```

### Gold Layer (Analytics - dbt)
```sql
gold.class_daily_performance      -- Daily metrics per class
gold.student_performance_summary  -- Overall student performance
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

### Run Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Copy raw data
cp /path/to/data/*.csv data/raw/
cp /path/to/data/*.json data/raw/

# Run ingestion
python src/main.py
```

### View Analytics

After running the pipeline, you can view the analytics tables using the `show_analytics.py` script:

```bash
python show_analytics.py
```

This displays:
- **Class Daily Performance** - Daily attendance rates and average scores per class
<img width="827" height="245" alt="image" src="https://github.com/user-attachments/assets/9f1d57fe-1d87-4fdb-988c-ab7528ef65a9" />


You can also import the functions for custom analysis:

```python
from show_analytics import (
    show_class_daily_performance,
    show_student_performance_summary,
    show_class_statistics
)

# Get data as DataFrame
df = show_class_daily_performance(limit=100)

# Filter or analyze further
df[df['class_id'] == 'CLASS-01']
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
