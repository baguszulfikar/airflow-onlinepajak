"""
Analytics Module
Builds Gold layer aggregations for reporting and analytics.
"""

from pathlib import Path
from typing import Optional

from ..utils.db import get_connection


def build_class_daily_performance(db_path: Optional[Path] = None) -> int:
    """
    Build the class_daily_performance Gold table.

    This table provides daily class-level metrics including:
    - Total students in class
    - Attendance statistics (present/absent counts, attendance rate)
    - Assessment statistics (count, average score)

    Schema:
        class_id VARCHAR - Unique class identifier (e.g., 'CLASS-01')
        date DATE - The date of the performance record
        total_students INTEGER - Total students enrolled in the class
        students_with_attendance INTEGER - Students who have attendance records
        present_count INTEGER - Number of students marked present
        absent_count INTEGER - Number of students marked absent
        attendance_rate DECIMAL(5,2) - Ratio of present to total attendance records
        students_with_assessment INTEGER - Distinct students with assessments
        assessment_count INTEGER - Total number of assessments taken
        avg_score DECIMAL(5,2) - Average assessment score

    Returns:
        Number of records in class_daily_performance
    """
    print("Building class_daily_performance...")

    with get_connection(db_path) as conn:
        # Clear existing data
        conn.execute("DELETE FROM gold.class_daily_performance")

        # Build the aggregation
        # We need to combine attendance and assessment data by class and date
        conn.execute("""
            INSERT INTO gold.class_daily_performance (
                class_id,
                date,
                total_students,
                students_with_attendance,
                present_count,
                absent_count,
                attendance_rate,
                students_with_assessment,
                assessment_count,
                avg_score,
                _created_at
            )
            WITH class_students AS (
                -- Get total students per class (active students only)
                SELECT
                    class_id,
                    COUNT(DISTINCT student_id) as total_students
                FROM silver.dim_students
                WHERE is_active = TRUE
                GROUP BY class_id
            ),
            daily_attendance AS (
                -- Get attendance metrics per class per day
                SELECT
                    class_id,
                    attendance_date as date,
                    COUNT(DISTINCT student_id) as students_with_attendance,
                    SUM(CASE WHEN is_present THEN 1 ELSE 0 END) as present_count,
                    SUM(CASE WHEN NOT is_present THEN 1 ELSE 0 END) as absent_count
                FROM silver.fact_attendance
                WHERE class_id != 'UNKNOWN'
                GROUP BY class_id, attendance_date
            ),
            daily_assessments AS (
                -- Get assessment metrics per class per day
                SELECT
                    class_id,
                    assessment_date as date,
                    COUNT(DISTINCT student_id) as students_with_assessment,
                    COUNT(*) as assessment_count,
                    ROUND(AVG(score_percentage), 2) as avg_score
                FROM silver.fact_assessments
                WHERE class_id != 'UNKNOWN'
                GROUP BY class_id, assessment_date
            ),
            all_class_dates AS (
                -- Union of all dates for each class
                SELECT DISTINCT class_id, date FROM daily_attendance
                UNION
                SELECT DISTINCT class_id, date FROM daily_assessments
            )
            SELECT
                acd.class_id,
                acd.date,
                COALESCE(cs.total_students, 0) as total_students,
                COALESCE(da.students_with_attendance, 0) as students_with_attendance,
                COALESCE(da.present_count, 0) as present_count,
                COALESCE(da.absent_count, 0) as absent_count,
                CASE
                    WHEN COALESCE(da.students_with_attendance, 0) > 0 THEN
                        ROUND(CAST(da.present_count AS DECIMAL) / da.students_with_attendance, 2)
                    ELSE 0
                END as attendance_rate,
                COALESCE(das.students_with_assessment, 0) as students_with_assessment,
                COALESCE(das.assessment_count, 0) as assessment_count,
                COALESCE(das.avg_score, 0) as avg_score,
                CURRENT_TIMESTAMP as _created_at
            FROM all_class_dates acd
            LEFT JOIN class_students cs ON acd.class_id = cs.class_id
            LEFT JOIN daily_attendance da ON acd.class_id = da.class_id AND acd.date = da.date
            LEFT JOIN daily_assessments das ON acd.class_id = das.class_id AND acd.date = das.date
            ORDER BY acd.class_id, acd.date
        """)

        count = conn.execute("SELECT COUNT(*) FROM gold.class_daily_performance").fetchone()[0]

    print(f"class_daily_performance: {count} records")
    return count


def build_student_performance_summary(db_path: Optional[Path] = None) -> int:
    """
    Build the student_performance_summary Gold table.

    This table provides student-level performance metrics including:
    - Attendance summary (days present/absent, rate)
    - Assessment summary (count, average/min/max scores)

    Returns:
        Number of records in student_performance_summary
    """
    print("Building student_performance_summary...")

    with get_connection(db_path) as conn:
        # Clear existing data
        conn.execute("DELETE FROM gold.student_performance_summary")

        # Build the aggregation
        conn.execute("""
            INSERT INTO gold.student_performance_summary (
                student_id,
                class_id,
                student_name,
                total_attendance_days,
                present_days,
                absent_days,
                attendance_rate,
                total_assessments,
                avg_score,
                min_score,
                max_score,
                _created_at
            )
            WITH attendance_summary AS (
                SELECT
                    student_id,
                    COUNT(*) as total_attendance_days,
                    SUM(CASE WHEN is_present THEN 1 ELSE 0 END) as present_days,
                    SUM(CASE WHEN NOT is_present THEN 1 ELSE 0 END) as absent_days
                FROM silver.fact_attendance
                GROUP BY student_id
            ),
            assessment_summary AS (
                SELECT
                    student_id,
                    COUNT(*) as total_assessments,
                    ROUND(AVG(score_percentage), 2) as avg_score,
                    MIN(score) as min_score,
                    MAX(score) as max_score
                FROM silver.fact_assessments
                GROUP BY student_id
            )
            SELECT
                s.student_id,
                s.class_id,
                s.student_name,
                COALESCE(att.total_attendance_days, 0) as total_attendance_days,
                COALESCE(att.present_days, 0) as present_days,
                COALESCE(att.absent_days, 0) as absent_days,
                CASE
                    WHEN COALESCE(att.total_attendance_days, 0) > 0 THEN
                        ROUND(CAST(att.present_days AS DECIMAL) / att.total_attendance_days, 2)
                    ELSE 0
                END as attendance_rate,
                COALESCE(asm.total_assessments, 0) as total_assessments,
                COALESCE(asm.avg_score, 0) as avg_score,
                COALESCE(asm.min_score, 0) as min_score,
                COALESCE(asm.max_score, 0) as max_score,
                CURRENT_TIMESTAMP as _created_at
            FROM silver.dim_students s
            LEFT JOIN attendance_summary att ON s.student_id = att.student_id
            LEFT JOIN assessment_summary asm ON s.student_id = asm.student_id
            WHERE s.is_active = TRUE
        """)

        count = conn.execute("SELECT COUNT(*) FROM gold.student_performance_summary").fetchone()[0]

    print(f"student_performance_summary: {count} records")
    return count


def run_analytics(db_path: Optional[Path] = None) -> dict:
    """
    Run all analytics aggregations from Silver to Gold layer.

    Returns:
        Dictionary with record counts for each table
    """
    print("\n" + "="*50)
    print("RUNNING ANALYTICS (Silver -> Gold)")
    print("="*50 + "\n")

    results = {
        'class_daily_performance': build_class_daily_performance(db_path),
        'student_performance_summary': build_student_performance_summary(db_path)
    }

    print("\nAnalytics build complete!")
    return results


# SQL DDL for class_daily_performance (for reference/documentation)
CLASS_DAILY_PERFORMANCE_DDL = """
-- Class Daily Performance Table
-- Purpose: Analytics-ready table for class performance reporting

CREATE TABLE IF NOT EXISTS gold.class_daily_performance (
    class_id                    VARCHAR         NOT NULL,   -- e.g., 'CLASS-01'
    date                        DATE            NOT NULL,   -- e.g., '2025-01-10'
    total_students              INTEGER         NOT NULL,   -- e.g., 30
    students_with_attendance    INTEGER         NOT NULL,   -- e.g., 28
    present_count               INTEGER         NOT NULL,   -- e.g., 25
    absent_count                INTEGER         NOT NULL,   -- e.g., 3
    attendance_rate             DECIMAL(5,2)    NOT NULL,   -- e.g., 0.83
    students_with_assessment    INTEGER         NOT NULL,   -- e.g., 18
    assessment_count            INTEGER         NOT NULL,   -- e.g., 22
    avg_score                   DECIMAL(5,2)    NOT NULL,   -- e.g., 74.5
    _created_at                 TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (class_id, date)
);

-- Example data:
-- class_id  | date       | total_students | students_with_attendance | present_count | absent_count | attendance_rate | students_with_assessment | assessment_count | avg_score
-- CLASS-01  | 2025-01-10 | 30             | 28                       | 25            | 3            | 0.83            | 18                       | 22               | 74.5
"""
