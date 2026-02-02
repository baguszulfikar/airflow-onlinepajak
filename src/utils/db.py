"""
Database utilities for the student data pipeline.
Uses DuckDB as the data warehouse.
"""

import duckdb
from pathlib import Path
from typing import Optional
import pandas as pd


# Default database path
DEFAULT_DB_PATH = Path(__file__).parent.parent.parent / "data" / "warehouse.duckdb"


def get_connection(db_path: Optional[Path] = None) -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection."""
    db_path = db_path or DEFAULT_DB_PATH
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def execute_query(query: str, db_path: Optional[Path] = None) -> None:
    """Execute a SQL query without returning results."""
    with get_connection(db_path) as conn:
        conn.execute(query)


def get_dataframe(query: str, db_path: Optional[Path] = None) -> pd.DataFrame:
    """Execute a query and return results as a DataFrame."""
    with get_connection(db_path) as conn:
        return conn.execute(query).fetchdf()


def init_schemas(db_path: Optional[Path] = None) -> None:
    """Initialize database schemas for bronze, silver, and gold layers."""
    with get_connection(db_path) as conn:
        # Create schemas
        conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
        conn.execute("CREATE SCHEMA IF NOT EXISTS gold")

        # Bronze layer tables
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bronze.students (
                student_id VARCHAR PRIMARY KEY,
                student_name VARCHAR,
                class_id VARCHAR,
                grade_level INTEGER,
                enrollment_status VARCHAR,
                updated_at TIMESTAMP,
                _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                _source_file VARCHAR
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS bronze.attendance (
                attendance_id VARCHAR PRIMARY KEY,
                student_id VARCHAR,
                attendance_date DATE,
                status VARCHAR,
                created_at TIMESTAMP,
                _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                _source_file VARCHAR
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS bronze.assessments (
                assessment_id VARCHAR PRIMARY KEY,
                student_id VARCHAR,
                subject VARCHAR,
                score INTEGER,
                max_score INTEGER,
                assessment_date DATE,
                created_at TIMESTAMP,
                _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                _source_file VARCHAR
            )
        """)

        # Silver layer tables
        conn.execute("""
            CREATE TABLE IF NOT EXISTS silver.dim_students (
                student_id VARCHAR PRIMARY KEY,
                student_name VARCHAR NOT NULL,
                class_id VARCHAR NOT NULL,
                grade_level INTEGER NOT NULL,
                enrollment_status VARCHAR NOT NULL,
                is_active BOOLEAN,
                updated_at TIMESTAMP,
                _processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS silver.fact_attendance (
                attendance_id VARCHAR PRIMARY KEY,
                student_id VARCHAR,
                class_id VARCHAR,
                attendance_date DATE NOT NULL,
                status VARCHAR NOT NULL,
                is_present BOOLEAN,
                _processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS silver.fact_assessments (
                assessment_id VARCHAR PRIMARY KEY,
                student_id VARCHAR,
                class_id VARCHAR,
                subject VARCHAR NOT NULL,
                score INTEGER NOT NULL,
                max_score INTEGER NOT NULL,
                score_percentage DECIMAL(5,2),
                assessment_date DATE NOT NULL,
                _processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Gold layer tables
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold.class_daily_performance (
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
                _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (class_id, date)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold.student_performance_summary (
                student_id VARCHAR,
                class_id VARCHAR,
                student_name VARCHAR,
                total_attendance_days INTEGER,
                present_days INTEGER,
                absent_days INTEGER,
                attendance_rate DECIMAL(5,2),
                total_assessments INTEGER,
                avg_score DECIMAL(5,2),
                min_score INTEGER,
                max_score INTEGER,
                _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (student_id)
            )
        """)

    print("Database schemas initialized successfully.")
