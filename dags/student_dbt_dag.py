"""
Student Data Pipeline - dbt DAG

This DAG handles data transformations using dbt:
1. Transform Bronze to Silver layer
2. Build Gold layer analytics
3. Run dbt tests
4. Validate output

Schedule: None (triggered by student_ingestion DAG)
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator


# Configuration
PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "data" / "warehouse.duckdb"

# dbt configuration
DBT_PROJECT_DIR = os.environ.get('DBT_PROJECT_DIR', '/opt/airflow/dbt/student-analytics-dbt')
DUCKDB_PATH = os.environ.get('DUCKDB_PATH', str(DB_PATH))


# Default DAG arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def validate_output_task():
    """Validate pipeline output."""
    import sys
    sys.path.insert(0, str(PROJECT_ROOT))
    from src.utils.db import get_connection

    with get_connection(DB_PATH) as conn:
        # Check that all Silver and Gold tables have data
        tables = [
            'silver.dim_students',
            'silver.fact_attendance',
            'silver.fact_assessments',
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
    'student_dbt',
    default_args=default_args,
    description='Student data transformations with dbt (Silver + Gold layers)',
    schedule_interval=None,  # Triggered by ingestion DAG
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['student', 'dbt', 'silver', 'gold', 'transformation'],
) as dag:

    # Start
    start = DummyOperator(task_id='start')

    # dbt deps - install packages
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir .',
        env={
            'DUCKDB_PATH': DUCKDB_PATH,
            'PATH': os.environ.get('PATH', ''),
        },
    )

    # dbt run - Silver layer
    dbt_run_silver = BashOperator(
        task_id='dbt_run_silver',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select silver --profiles-dir .',
        env={
            'DUCKDB_PATH': DUCKDB_PATH,
            'PATH': os.environ.get('PATH', ''),
        },
    )

    # dbt test - Silver layer
    dbt_test_silver = BashOperator(
        task_id='dbt_test_silver',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --select silver --profiles-dir .',
        env={
            'DUCKDB_PATH': DUCKDB_PATH,
            'PATH': os.environ.get('PATH', ''),
        },
    )

    # dbt run - Gold layer
    dbt_run_gold = BashOperator(
        task_id='dbt_run_gold',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select gold --profiles-dir .',
        env={
            'DUCKDB_PATH': DUCKDB_PATH,
            'PATH': os.environ.get('PATH', ''),
        },
    )

    # dbt test - Gold layer
    dbt_test_gold = BashOperator(
        task_id='dbt_test_gold',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --select gold --profiles-dir .',
        env={
            'DUCKDB_PATH': DUCKDB_PATH,
            'PATH': os.environ.get('PATH', ''),
        },
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
    # start ─► dbt_deps ─► dbt_run_silver ─► dbt_test_silver ─► dbt_run_gold ─► dbt_test_gold ─► validate ─► end
    #
    start >> dbt_deps >> dbt_run_silver >> dbt_test_silver >> dbt_run_gold >> dbt_test_gold >> validate_output >> end
