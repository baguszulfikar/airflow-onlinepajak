"""
Student Data Pipeline - Ingestion DAG

This DAG handles data ingestion into the Bronze layer:
1. Initialize database schemas
2. Ingest students, attendance, and assessments data

Schedule: Daily at 6:00 AM
Triggers: student_dbt_dag after completion
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


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
    """Initialize database schemas (Bronze layer only)."""
    import sys
    sys.path.insert(0, str(PROJECT_ROOT))
    from src.utils.db import get_connection

    with get_connection(DB_PATH) as conn:
        # Create Bronze schema
        conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")

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

    print("Bronze schemas initialized successfully.")


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


# Define DAG
with DAG(
    'student_ingestion',
    default_args=default_args,
    description='Student data ingestion pipeline (Bronze layer)',
    schedule_interval='0 6 * * *',  # Daily at 6:00 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['student', 'ingestion', 'bronze'],
) as dag:

    # Start
    start = DummyOperator(task_id='start')

    # Initialize database (Bronze schema only)
    init_db = PythonOperator(
        task_id='init_database',
        python_callable=init_database,
    )

    # Ingestion tasks (sequential to avoid DuckDB lock conflicts)
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

    # Trigger dbt DAG after ingestion completes
    trigger_dbt = TriggerDagRunOperator(
        task_id='trigger_dbt_dag',
        trigger_dag_id='student_dbt',
        wait_for_completion=False,
        poke_interval=30,
    )

    # End
    end = DummyOperator(task_id='end')

    # Define task dependencies
    # DAG Structure:
    #
    # start ─► init_db ─► ingest_students ─► ingest_attendance ─► ingest_assessments ─► trigger_dbt ─► end
    #
    start >> init_db >> ingest_students >> ingest_attendance >> ingest_assessments >> trigger_dbt >> end
