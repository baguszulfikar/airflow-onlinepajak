"""
Tests for Student Data Pipeline
"""

import pytest
import pandas as pd
from pathlib import Path
import tempfile
import json
import sys

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture
def temp_db(tmp_path):
    """Create a temporary database for testing."""
    db_path = tmp_path / "test_warehouse.duckdb"
    yield db_path
    # Cleanup
    if db_path.exists():
        db_path.unlink()


@pytest.fixture
def sample_data_dir(tmp_path):
    """Create sample data files for testing."""
    # Students CSV
    students_data = """student_id,student_name,class_id,grade_level,enrollment_status,updated_at
S-0001,John Doe,CLASS-01,10,ACTIVE,2025-01-01T00:00:00
S-0002,Jane Smith,CLASS-01,10,ACTIVE,2025-01-01T00:00:00
S-0003,Bob Wilson,CLASS-02,11,ACTIVE,2025-01-01T00:00:00"""
    (tmp_path / "students.csv").write_text(students_data)

    # Attendance CSV
    attendance_data = """attendance_id,student_id,attendance_date,status,created_at
ATT-00001,S-0001,2025-01-10,PRESENT,2025-01-10T00:00:00
ATT-00002,S-0002,2025-01-10,PRESENT,2025-01-10T00:00:00
ATT-00003,S-0003,2025-01-10,ABSENT,2025-01-10T00:00:00"""
    (tmp_path / "attendance.csv").write_text(attendance_data)

    # Assessments JSON
    assessments_data = [
        {"assessment_id": "ASM-00001", "student_id": "S-0001", "subject": "Math",
         "score": 85, "max_score": 100, "assessment_date": "2025-01-10", "created_at": "2025-01-10T01:00:00"},
        {"assessment_id": "ASM-00002", "student_id": "S-0002", "subject": "Math",
         "score": 90, "max_score": 100, "assessment_date": "2025-01-10", "created_at": "2025-01-10T01:00:00"}
    ]
    (tmp_path / "assessments.json").write_text(json.dumps(assessments_data))

    return tmp_path


def test_init_schemas(temp_db):
    """Test database schema initialization."""
    from src.utils.db import init_schemas, get_connection

    init_schemas(temp_db)

    with get_connection(temp_db) as conn:
        # Check that tables exist
        tables = conn.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema IN ('bronze', 'silver', 'gold')
        """).fetchdf()

        assert len(tables) >= 6  # At least 6 tables created


def test_ingest_students(temp_db, sample_data_dir):
    """Test student data ingestion."""
    from src.utils.db import init_schemas
    from src.ingestion import ingest_students

    init_schemas(temp_db)
    count = ingest_students(sample_data_dir / "students.csv", temp_db)

    assert count == 3


def test_ingest_attendance(temp_db, sample_data_dir):
    """Test attendance data ingestion."""
    from src.utils.db import init_schemas
    from src.ingestion import ingest_attendance

    init_schemas(temp_db)
    count = ingest_attendance(sample_data_dir / "attendance.csv", temp_db)

    assert count == 3


def test_ingest_assessments(temp_db, sample_data_dir):
    """Test assessments data ingestion."""
    from src.utils.db import init_schemas
    from src.ingestion import ingest_assessments

    init_schemas(temp_db)
    count = ingest_assessments(sample_data_dir / "assessments.json", temp_db)

    assert count == 2


def test_transformations(temp_db, sample_data_dir):
    """Test data transformations."""
    from src.utils.db import init_schemas, get_connection
    from src.ingestion import ingest_students, ingest_attendance, ingest_assessments
    from src.transformation import run_transformations

    # Setup
    init_schemas(temp_db)
    ingest_students(sample_data_dir / "students.csv", temp_db)
    ingest_attendance(sample_data_dir / "attendance.csv", temp_db)
    ingest_assessments(sample_data_dir / "assessments.json", temp_db)

    # Transform
    results = run_transformations(temp_db)

    assert results['dim_students'] == 3
    assert results['fact_attendance'] == 3
    assert results['fact_assessments'] == 2


def test_analytics(temp_db, sample_data_dir):
    """Test analytics aggregations."""
    from src.utils.db import init_schemas, get_connection
    from src.ingestion import ingest_students, ingest_attendance, ingest_assessments
    from src.transformation import run_transformations
    from src.analytics import run_analytics

    # Setup
    init_schemas(temp_db)
    ingest_students(sample_data_dir / "students.csv", temp_db)
    ingest_attendance(sample_data_dir / "attendance.csv", temp_db)
    ingest_assessments(sample_data_dir / "assessments.json", temp_db)
    run_transformations(temp_db)

    # Build analytics
    results = run_analytics(temp_db)

    assert results['class_daily_performance'] > 0
    assert results['student_performance_summary'] == 3

    # Verify class_daily_performance data
    with get_connection(temp_db) as conn:
        df = conn.execute("""
            SELECT * FROM gold.class_daily_performance
            WHERE class_id = 'CLASS-01' AND date = '2025-01-10'
        """).fetchdf()

        if not df.empty:
            row = df.iloc[0]
            assert row['present_count'] == 2
            assert row['absent_count'] == 0


def test_full_pipeline(temp_db, sample_data_dir):
    """Test complete pipeline execution."""
    from src.main import run_pipeline

    results = run_pipeline(sample_data_dir, temp_db)

    assert 'ingestion' in results
    assert 'transformation' in results
    assert 'analytics' in results
    assert results['duration'] is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
