"""
Query Results Helper

This script provides easy access to query the pipeline results.

Usage:
    python src/query_results.py
    python src/query_results.py --table class_daily_performance
    python src/query_results.py --sql "SELECT * FROM gold.class_daily_performance LIMIT 10"
"""

import argparse
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.db import get_connection, get_dataframe


def show_table_samples(db_path: Path = None):
    """Show sample data from all layers."""
    print("\n" + "="*70)
    print("DATABASE OVERVIEW")
    print("="*70)

    with get_connection(db_path) as conn:
        # Bronze layer
        print("\n" + "-"*50)
        print("BRONZE LAYER (Raw Data)")
        print("-"*50)

        tables = ['bronze.students', 'bronze.attendance', 'bronze.assessments']
        for table in tables:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"\n{table}: {count} records")
            df = conn.execute(f"SELECT * FROM {table} LIMIT 3").fetchdf()
            print(df.to_string(index=False))

        # Silver layer
        print("\n" + "-"*50)
        print("SILVER LAYER (Cleaned Data)")
        print("-"*50)

        tables = ['silver.dim_students', 'silver.fact_attendance', 'silver.fact_assessments']
        for table in tables:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"\n{table}: {count} records")
            df = conn.execute(f"SELECT * FROM {table} LIMIT 3").fetchdf()
            print(df.to_string(index=False))

        # Gold layer
        print("\n" + "-"*50)
        print("GOLD LAYER (Analytics)")
        print("-"*50)

        print("\ngold.class_daily_performance:")
        count = conn.execute("SELECT COUNT(*) FROM gold.class_daily_performance").fetchone()[0]
        print(f"Total records: {count}")
        df = conn.execute("""
            SELECT
                class_id,
                date,
                total_students,
                students_with_attendance,
                present_count,
                absent_count,
                attendance_rate,
                students_with_assessment,
                assessment_count,
                avg_score
            FROM gold.class_daily_performance
            ORDER BY class_id, date
            LIMIT 10
        """).fetchdf()
        print(df.to_string(index=False))

        print("\ngold.student_performance_summary:")
        count = conn.execute("SELECT COUNT(*) FROM gold.student_performance_summary").fetchone()[0]
        print(f"Total records: {count}")
        df = conn.execute("""
            SELECT
                student_id,
                class_id,
                student_name,
                total_attendance_days,
                present_days,
                attendance_rate,
                total_assessments,
                avg_score
            FROM gold.student_performance_summary
            ORDER BY class_id, student_id
            LIMIT 10
        """).fetchdf()
        print(df.to_string(index=False))


def run_custom_query(sql: str, db_path: Path = None):
    """Execute a custom SQL query."""
    print(f"\nExecuting: {sql}")
    print("-"*50)

    df = get_dataframe(sql, db_path)
    print(df.to_string(index=False))
    print(f"\n{len(df)} rows returned")


def show_class_performance(class_id: str = None, db_path: Path = None):
    """Show class daily performance data."""
    with get_connection(db_path) as conn:
        if class_id:
            df = conn.execute(f"""
                SELECT *
                FROM gold.class_daily_performance
                WHERE class_id = '{class_id}'
                ORDER BY date
            """).fetchdf()
            print(f"\nClass Performance for {class_id}:")
        else:
            df = conn.execute("""
                SELECT *
                FROM gold.class_daily_performance
                ORDER BY class_id, date
            """).fetchdf()
            print("\nAll Class Performance:")

        print(df.to_string(index=False))


def main():
    parser = argparse.ArgumentParser(description='Query Pipeline Results')
    parser.add_argument(
        '--table',
        type=str,
        choices=['class_daily_performance', 'student_performance_summary', 'all'],
        default='all',
        help='Table to query'
    )
    parser.add_argument(
        '--sql',
        type=str,
        help='Custom SQL query to execute'
    )
    parser.add_argument(
        '--class-id',
        type=str,
        help='Filter by class ID (e.g., CLASS-01)'
    )
    parser.add_argument(
        '--db-path',
        type=Path,
        default=None,
        help='Path to DuckDB database file'
    )

    args = parser.parse_args()

    try:
        if args.sql:
            run_custom_query(args.sql, args.db_path)
        elif args.class_id:
            show_class_performance(args.class_id, args.db_path)
        else:
            show_table_samples(args.db_path)

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
