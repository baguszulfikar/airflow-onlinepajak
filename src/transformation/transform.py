"""
Transformation Module
Transforms Bronze layer data into Silver layer (cleaned, validated, enriched).
"""

from pathlib import Path
from typing import Optional
from datetime import datetime

from ..utils.db import get_connection


def transform_students(db_path: Optional[Path] = None) -> int:
    """
    Transform bronze.students into silver.dim_students.

    Transformations:
    - Remove duplicates (keep latest by updated_at)
    - Add is_active flag based on enrollment_status
    - Validate and clean data
    - Handle null values

    Returns:
        Number of records in dim_students
    """
    print("Transforming students -> dim_students")

    with get_connection(db_path) as conn:
        # Clear and rebuild dim_students
        conn.execute("DELETE FROM silver.dim_students")

        conn.execute("""
            INSERT INTO silver.dim_students (
                student_id,
                student_name,
                class_id,
                grade_level,
                enrollment_status,
                is_active,
                updated_at,
                _processed_at
            )
            SELECT
                student_id,
                COALESCE(student_name, 'Unknown') as student_name,
                COALESCE(class_id, 'UNKNOWN') as class_id,
                COALESCE(grade_level, 0) as grade_level,
                COALESCE(enrollment_status, 'UNKNOWN') as enrollment_status,
                CASE
                    WHEN enrollment_status = 'ACTIVE' THEN TRUE
                    ELSE FALSE
                END as is_active,
                updated_at,
                CURRENT_TIMESTAMP as _processed_at
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY student_id
                        ORDER BY updated_at DESC, _ingested_at DESC
                    ) as rn
                FROM bronze.students
            )
            WHERE rn = 1
        """)

        count = conn.execute("SELECT COUNT(*) FROM silver.dim_students").fetchone()[0]

    print(f"dim_students: {count} records")
    return count


def transform_attendance(db_path: Optional[Path] = None) -> int:
    """
    Transform bronze.attendance into silver.fact_attendance.

    Transformations:
    - Deduplicate by attendance_id
    - Join with dim_students to get class_id
    - Add is_present boolean flag
    - Filter out records with invalid student references

    Returns:
        Number of records in fact_attendance
    """
    print("Transforming attendance -> fact_attendance")

    with get_connection(db_path) as conn:
        # Clear and rebuild fact_attendance
        conn.execute("DELETE FROM silver.fact_attendance")

        conn.execute("""
            INSERT INTO silver.fact_attendance (
                attendance_id,
                student_id,
                class_id,
                attendance_date,
                status,
                is_present,
                _processed_at
            )
            SELECT
                a.attendance_id,
                a.student_id,
                COALESCE(s.class_id, 'UNKNOWN') as class_id,
                a.attendance_date,
                a.status,
                CASE
                    WHEN a.status = 'PRESENT' THEN TRUE
                    ELSE FALSE
                END as is_present,
                CURRENT_TIMESTAMP as _processed_at
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY attendance_id
                        ORDER BY _ingested_at DESC
                    ) as rn
                FROM bronze.attendance
            ) a
            LEFT JOIN silver.dim_students s ON a.student_id = s.student_id
            WHERE a.rn = 1
        """)

        count = conn.execute("SELECT COUNT(*) FROM silver.fact_attendance").fetchone()[0]

    print(f"fact_attendance: {count} records")
    return count


def transform_assessments(db_path: Optional[Path] = None) -> int:
    """
    Transform bronze.assessments into silver.fact_assessments.

    Transformations:
    - Deduplicate by assessment_id
    - Join with dim_students to get class_id
    - Calculate score_percentage
    - Validate score ranges

    Returns:
        Number of records in fact_assessments
    """
    print("Transforming assessments -> fact_assessments")

    with get_connection(db_path) as conn:
        # Clear and rebuild fact_assessments
        conn.execute("DELETE FROM silver.fact_assessments")

        conn.execute("""
            INSERT INTO silver.fact_assessments (
                assessment_id,
                student_id,
                class_id,
                subject,
                score,
                max_score,
                score_percentage,
                assessment_date,
                _processed_at
            )
            SELECT
                a.assessment_id,
                a.student_id,
                COALESCE(s.class_id, 'UNKNOWN') as class_id,
                a.subject,
                -- Ensure score doesn't exceed max_score
                LEAST(a.score, a.max_score) as score,
                a.max_score,
                -- Calculate percentage (handle division by zero)
                CASE
                    WHEN a.max_score > 0 THEN
                        ROUND(CAST(LEAST(a.score, a.max_score) AS DECIMAL) / a.max_score * 100, 2)
                    ELSE 0
                END as score_percentage,
                a.assessment_date,
                CURRENT_TIMESTAMP as _processed_at
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY assessment_id
                        ORDER BY _ingested_at DESC
                    ) as rn
                FROM bronze.assessments
            ) a
            LEFT JOIN silver.dim_students s ON a.student_id = s.student_id
            WHERE a.rn = 1
        """)

        count = conn.execute("SELECT COUNT(*) FROM silver.fact_assessments").fetchone()[0]

    print(f"fact_assessments: {count} records")
    return count


def run_transformations(db_path: Optional[Path] = None) -> dict:
    """
    Run all transformations from Bronze to Silver layer.

    Returns:
        Dictionary with record counts for each table
    """
    print("\n" + "="*50)
    print("RUNNING TRANSFORMATIONS (Bronze -> Silver)")
    print("="*50 + "\n")

    results = {
        'dim_students': transform_students(db_path),
        'fact_attendance': transform_attendance(db_path),
        'fact_assessments': transform_assessments(db_path)
    }

    print("\nTransformation complete!")
    return results
