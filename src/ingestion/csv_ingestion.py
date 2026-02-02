"""
CSV Ingestion Module
Handles ingestion of CSV files (students.csv, attendance.csv) into the Bronze layer.
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Optional
import duckdb

from ..utils.db import get_connection


def ingest_students(
    file_path: Path,
    db_path: Optional[Path] = None,
    source_name: Optional[str] = None
) -> int:
    """
    Ingest students data from CSV into bronze.students table.

    Strategy: Full Load with Upsert
    - Loads entire file
    - Upserts based on student_id (primary key)
    - Preserves data lineage with metadata columns

    Args:
        file_path: Path to students.csv
        db_path: Optional path to DuckDB database
        source_name: Optional source identifier

    Returns:
        Number of records processed
    """
    print(f"Ingesting students from: {file_path}")

    # Read CSV with proper dtypes
    df = pd.read_csv(
        file_path,
        dtype={
            'student_id': str,
            'student_name': str,
            'class_id': str,
            'grade_level': int,
            'enrollment_status': str
        },
        parse_dates=['updated_at']
    )

    # Data validation
    required_columns = ['student_id', 'student_name', 'class_id', 'grade_level',
                        'enrollment_status', 'updated_at']
    missing_cols = set(required_columns) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Validate student_id format (S-XXXX)
    invalid_ids = df[~df['student_id'].str.match(r'^S-\d{4}$', na=False)]
    if not invalid_ids.empty:
        print(f"Warning: {len(invalid_ids)} records with invalid student_id format")

    # Add metadata columns
    df['_ingested_at'] = datetime.now()
    df['_source_file'] = source_name or file_path.name

    # Upsert into bronze layer
    with get_connection(db_path) as conn:
        # Create temp table and upsert
        conn.register('students_staging', df)

        conn.execute("""
            INSERT OR REPLACE INTO bronze.students
            SELECT
                student_id,
                student_name,
                class_id,
                grade_level,
                enrollment_status,
                updated_at,
                _ingested_at,
                _source_file
            FROM students_staging
        """)

    record_count = len(df)
    print(f"Successfully ingested {record_count} student records")
    return record_count


def ingest_attendance(
    file_path: Path,
    db_path: Optional[Path] = None,
    source_name: Optional[str] = None
) -> int:
    """
    Ingest attendance data from CSV into bronze.attendance table.

    Strategy: Incremental Load with Append
    - Appends new records
    - Deduplicates by attendance_id
    - Preserves data lineage with metadata columns

    Args:
        file_path: Path to attendance.csv
        db_path: Optional path to DuckDB database
        source_name: Optional source identifier

    Returns:
        Number of new records ingested
    """
    print(f"Ingesting attendance from: {file_path}")

    # Read CSV with proper dtypes
    df = pd.read_csv(
        file_path,
        dtype={
            'attendance_id': str,
            'student_id': str,
            'status': str
        },
        parse_dates=['attendance_date', 'created_at']
    )

    # Data validation
    required_columns = ['attendance_id', 'student_id', 'attendance_date', 'status', 'created_at']
    missing_cols = set(required_columns) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Validate status values
    valid_statuses = {'PRESENT', 'ABSENT'}
    invalid_statuses = set(df['status'].unique()) - valid_statuses
    if invalid_statuses:
        print(f"Warning: Invalid status values found: {invalid_statuses}")

    # Add metadata columns
    df['_ingested_at'] = datetime.now()
    df['_source_file'] = source_name or file_path.name

    # Insert with deduplication (ignore duplicates)
    with get_connection(db_path) as conn:
        conn.register('attendance_staging', df)

        # Get count of existing records
        existing_count = conn.execute(
            "SELECT COUNT(*) FROM bronze.attendance"
        ).fetchone()[0]

        # Insert only new records (by attendance_id)
        conn.execute("""
            INSERT OR IGNORE INTO bronze.attendance
            SELECT
                attendance_id,
                student_id,
                attendance_date,
                status,
                created_at,
                _ingested_at,
                _source_file
            FROM attendance_staging
            WHERE attendance_id NOT IN (SELECT attendance_id FROM bronze.attendance)
        """)

        new_count = conn.execute(
            "SELECT COUNT(*) FROM bronze.attendance"
        ).fetchone()[0]

    records_added = new_count - existing_count
    print(f"Successfully ingested {records_added} new attendance records (total: {new_count})")
    return records_added
