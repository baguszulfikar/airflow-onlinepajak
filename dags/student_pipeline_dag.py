"""
Student Data Pipeline - Airflow DAG

This DAG orchestrates the end-to-end student data pipeline:
1. Ingest data from sources (Bronze layer)
2. Transform and clean data (Silver layer)
3. Build analytics aggregations (Gold layer)

Schedule: Daily at 6:00 AM
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator


# Configuration
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "raw"
DB_PATH = PROJECT_ROOT / "data" / "warehouse.duckdb"


# Default DAG arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def init_database():
    """Initialize database schemas."""
    import sys
    sys.path.insert(0, str(PROJECT_ROOT))
    from src.utils.db import init_schemas
    init_schemas(DB_PATH)


def ingest_students_task():
    """Ingest students data from CSV."""
    import sys
    sys.path.insert(0, str(PROJECT_ROOT))
    from src.ingestion import ingest_students
    ingest_students(DATA_DIR / "students.csv", DB_PATH)


def ingest_attendance_task():
    """Ingest attendance data from CSV."""
    import sys
    sys.path.insert(0, str(PROJECT_ROOT))
    from src.ingestion import ingest_attendance
    ingest_attendance(DATA_DIR / "attendance.csv", DB_PATH)


def ingest_assessments_task():
    """Ingest assessments data from JSON."""
    import sys
    sys.path.insert(0, str(PROJECT_ROOT))
    from src.ingestion import ingest_assessments
    ingest_assessments(DATA_DIR / "assessments.json", DB_PATH)


def transform_data_task():
    """Transform Bronze to Silver layer."""
    import sys
    sys.path.insert(0, str(PROJECT_ROOT))
    from src.transformation import run_transformations
    run_transformations(DB_PATH)


def build_analytics_task():
    """Build Gold layer analytics."""
    import sys
    sys.path.insert(0, str(PROJECT_ROOT))
    from src.analytics import run_analytics
    run_analytics(DB_PATH)


def validate_output_task():
    """Validate pipeline output."""
    import sys
    sys.path.insert(0, str(PROJECT_ROOT))
    from src.utils.db import get_connection

    with get_connection(DB_PATH) as conn:
        # Check that all Gold tables have data
        tables = [
            'gold.class_daily_performance',
            'gold.student_performance_summary'
        ]

        for table in tables:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            if count == 0:
                raise ValueError(f"Table {table} is empty!")
            print(f"Validation passed: {table} has {count} records")


# Define DAG
with DAG(
    'student_data_pipeline',
    default_args=default_args,
    description='End-to-end student data pipeline',
    schedule_interval='0 6 * * *',  # Daily at 6:00 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['student', 'data-pipeline', 'analytics'],
) as dag:

    # Start
    start = DummyOperator(task_id='start')

    # Initialize database
    init_db = PythonOperator(
        task_id='init_database',
        python_callable=init_database,
    )

    # Ingestion tasks (can run in parallel)
    ingest_students = PythonOperator(
        task_id='ingest_students',
        python_callable=ingest_students_task,
    )

    ingest_attendance = PythonOperator(
        task_id='ingest_attendance',
        python_callable=ingest_attendance_task,
    )

    ingest_assessments = PythonOperator(
        task_id='ingest_assessments',
        python_callable=ingest_assessments_task,
    )

    # Wait for all ingestion to complete
    ingestion_complete = DummyOperator(task_id='ingestion_complete')

    # Transformation
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data_task,
    )

    # Analytics
    build_analytics = PythonOperator(
        task_id='build_analytics',
        python_callable=build_analytics_task,
    )

    # Validation
    validate_output = PythonOperator(
        task_id='validate_output',
        python_callable=validate_output_task,
    )

    # End
    end = DummyOperator(task_id='end')

    # Define task dependencies
    # DAG Structure:
    #
    #                    ┌─ ingest_students ──┐
    # start ─► init_db ──┼─ ingest_attendance ┼─► ingestion_complete ─► transform ─► analytics ─► validate ─► end
    #                    └─ ingest_assessments┘
    #
    start >> init_db

    init_db >> [ingest_students, ingest_attendance, ingest_assessments]

    [ingest_students, ingest_attendance, ingest_assessments] >> ingestion_complete

    ingestion_complete >> transform_data >> build_analytics >> validate_output >> end
