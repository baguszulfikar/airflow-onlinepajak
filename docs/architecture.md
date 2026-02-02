# Data Pipeline Architecture

## High-Level Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              STUDENT DATA PIPELINE                                       │
│                                                                                          │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │                              DATA SOURCES                                        │   │
│   │                                                                                  │   │
│   │    ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐                 │   │
│   │    │ students.csv │    │attendance.csv│    │ assessments.json │                 │   │
│   │    │              │    │              │    │                  │                 │   │
│   │    │  • student_id│    │ • attend_id  │    │  • assessment_id │                 │   │
│   │    │  • name      │    │ • student_id │    │  • student_id    │                 │   │
│   │    │  • class_id  │    │ • date       │    │  • subject       │                 │   │
│   │    │  • grade     │    │ • status     │    │  • score         │                 │   │
│   │    │  • status    │    │              │    │  • date          │                 │   │
│   │    └──────┬───────┘    └──────┬───────┘    └────────┬─────────┘                 │   │
│   │           │                   │                     │                            │   │
│   └───────────┼───────────────────┼─────────────────────┼────────────────────────────┘   │
│               │                   │                     │                                │
│               ▼                   ▼                     ▼                                │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │                         BRONZE LAYER (Raw Data)                                  │   │
│   │                                                                                  │   │
│   │    ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐               │   │
│   │    │ bronze.students  │ │ bronze.attendance│ │bronze.assessments│               │   │
│   │    │                  │ │                  │ │                  │               │   │
│   │    │ Strategy:        │ │ Strategy:        │ │ Strategy:        │               │   │
│   │    │ Full Load +      │ │ Incremental      │ │ Incremental      │               │   │
│   │    │ Upsert           │ │ Append           │ │ Append           │               │   │
│   │    │                  │ │                  │ │                  │               │   │
│   │    │ + _ingested_at   │ │ + _ingested_at   │ │ + _ingested_at   │               │   │
│   │    │ + _source_file   │ │ + _source_file   │ │ + _source_file   │               │   │
│   │    └────────┬─────────┘ └────────┬─────────┘ └────────┬─────────┘               │   │
│   │             │                    │                    │                          │   │
│   └─────────────┼────────────────────┼────────────────────┼──────────────────────────┘   │
│                 │                    │                    │                              │
│                 └──────────────┬─────┴────────────────────┘                              │
│                                │                                                         │
│                                ▼                                                         │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │                        SILVER LAYER (Cleaned Data)                               │   │
│   │                                                                                  │   │
│   │    Transformations:                                                              │   │
│   │    • Deduplication (by primary key)                                              │   │
│   │    • Null handling (COALESCE defaults)                                           │   │
│   │    • Data validation                                                             │   │
│   │    • Type standardization                                                        │   │
│   │    • Referential integrity (JOIN to get class_id)                                │   │
│   │                                                                                  │   │
│   │    ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐               │   │
│   │    │silver.dim_student│ │silver.fact_attend│ │silver.fact_assess│               │   │
│   │    │                  │ │                  │ │                  │               │   │
│   │    │ • student_id (PK)│ │ • attend_id (PK) │ │ • assess_id (PK) │               │   │
│   │    │ • student_name   │ │ • student_id     │ │ • student_id     │               │   │
│   │    │ • class_id       │ │ • class_id       │ │ • class_id       │               │   │
│   │    │ • grade_level    │ │ • attend_date    │ │ • subject        │               │   │
│   │    │ • is_active      │ │ • status         │ │ • score          │               │   │
│   │    │                  │ │ • is_present     │ │ • score_pct      │               │   │
│   │    └────────┬─────────┘ └────────┬─────────┘ └────────┬─────────┘               │   │
│   │             │                    │                    │                          │   │
│   └─────────────┼────────────────────┼────────────────────┼──────────────────────────┘   │
│                 │                    │                    │                              │
│                 └──────────────┬─────┴────────────────────┘                              │
│                                │                                                         │
│                                ▼                                                         │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │                          GOLD LAYER (Analytics)                                  │   │
│   │                                                                                  │   │
│   │    Aggregations:                                                                 │   │
│   │    • JOIN fact tables with dimensions                                            │   │
│   │    • GROUP BY class_id + date                                                    │   │
│   │    • Calculate KPIs (rates, averages)                                            │   │
│   │                                                                                  │   │
│   │    ┌─────────────────────────────────┐  ┌─────────────────────────────────┐     │   │
│   │    │gold.class_daily_performance     │  │gold.student_performance_summary │     │   │
│   │    │                                 │  │                                 │     │   │
│   │    │ • class_id + date (PK)          │  │ • student_id (PK)               │     │   │
│   │    │ • total_students                │  │ • class_id                      │     │   │
│   │    │ • students_with_attendance      │  │ • total_attendance_days         │     │   │
│   │    │ • present_count                 │  │ • present_days                  │     │   │
│   │    │ • absent_count                  │  │ • attendance_rate               │     │   │
│   │    │ • attendance_rate               │  │ • total_assessments             │     │   │
│   │    │ • students_with_assessment      │  │ • avg_score                     │     │   │
│   │    │ • assessment_count              │  │ • min_score                     │     │   │
│   │    │ • avg_score                     │  │ • max_score                     │     │   │
│   │    └─────────────────────────────────┘  └─────────────────────────────────┘     │   │
│   │                                                                                  │   │
│   └──────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow Diagram

```
                    ┌─────────────────────────────────────────────────────────┐
                    │                    AIRFLOW DAG                          │
                    │             student_data_pipeline                       │
                    │             Schedule: 0 6 * * *                         │
                    └─────────────────────────────────────────────────────────┘
                                              │
                                              ▼
                    ┌─────────────────────────────────────────────────────────┐
                    │                    init_database                        │
                    │              Create schemas if not exist                │
                    └─────────────────────────────────────────────────────────┘
                                              │
           ┌──────────────────────────────────┼──────────────────────────────────┐
           │                                  │                                  │
           ▼                                  ▼                                  ▼
┌─────────────────────┐         ┌─────────────────────┐         ┌─────────────────────┐
│  ingest_students    │         │ ingest_attendance   │         │ ingest_assessments  │
│                     │         │                     │         │                     │
│  CSV → Bronze       │         │  CSV → Bronze       │         │  JSON → Bronze      │
│  Full Load + Upsert │         │  Incremental Append │         │  Incremental Append │
└─────────────────────┘         └─────────────────────┘         └─────────────────────┘
           │                                  │                                  │
           └──────────────────────────────────┼──────────────────────────────────┘
                                              │
                                              ▼
                    ┌─────────────────────────────────────────────────────────┐
                    │                  transform_data                         │
                    │                                                         │
                    │  Bronze → Silver                                        │
                    │  • Deduplicate                                          │
                    │  • Clean & validate                                     │
                    │  • Enrich (add class_id, is_present flags)              │
                    └─────────────────────────────────────────────────────────┘
                                              │
                                              ▼
                    ┌─────────────────────────────────────────────────────────┐
                    │                  build_analytics                        │
                    │                                                         │
                    │  Silver → Gold                                          │
                    │  • Aggregate by class + date                            │
                    │  • Calculate KPIs                                       │
                    │  • Build summary tables                                 │
                    └─────────────────────────────────────────────────────────┘
                                              │
                                              ▼
                    ┌─────────────────────────────────────────────────────────┐
                    │                  validate_output                        │
                    │                                                         │
                    │  • Check tables not empty                               │
                    │  • Data quality checks                                  │
                    └─────────────────────────────────────────────────────────┘
```

## Technology Stack

| Component | Technology | Rationale |
|-----------|------------|-----------|
| Data Processing | Python + Pandas | Flexible, well-known, extensive ecosystem |
| Data Warehouse | DuckDB | Lightweight, SQL-based, no external dependencies, fast columnar storage |
| Orchestration | Apache Airflow | Industry standard, DAG-based scheduling, monitoring UI |
| Containerization | Docker | Portable, reproducible environments |
| Testing | pytest | Standard Python testing framework |

## Design Decisions

### 1. Medallion Architecture (Bronze → Silver → Gold)
- **Why**: Clear separation of concerns, supports incremental processing, provides audit trail
- **Bronze**: Raw data as-is, with ingestion metadata
- **Silver**: Cleaned, validated, business logic applied
- **Gold**: Pre-aggregated for analytics

### 2. DuckDB for Data Warehouse
- **Why**:
  - No external database server required
  - Embedded analytics database
  - Excellent performance for OLAP queries
  - SQL-native interface
  - Easy to deploy

### 3. Incremental vs Full Load
- **Students**: Full load because it's a dimension that changes slowly
- **Attendance/Assessments**: Incremental because these are append-only facts

### 4. Airflow for Orchestration
- **Why**:
  - Industry standard for data pipelines
  - DAG visualization
  - Built-in retry logic
  - Monitoring and alerting
  - Parallel task execution
