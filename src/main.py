"""
Student Data Pipeline - Main Entry Point

This script orchestrates the end-to-end data pipeline:
1. Initialize database schemas
2. Ingest data from sources (Bronze layer)
3. Transform and clean data (Silver layer)
4. Build analytics aggregations (Gold layer)

Usage:
    python src/main.py
    python src/main.py --data-dir /path/to/data
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.db import init_schemas, get_connection
from src.ingestion import ingest_students, ingest_attendance, ingest_assessments
from src.transformation import run_transformations
from src.analytics import run_analytics


def run_pipeline(data_dir: Path, db_path: Path = None) -> dict:
    """
    Run the complete data pipeline.

    Args:
        data_dir: Directory containing source data files
        db_path: Optional path to DuckDB database

    Returns:
        Dictionary with pipeline execution results
    """
    start_time = datetime.now()
    results = {
        'start_time': start_time,
        'ingestion': {},
        'transformation': {},
        'analytics': {}
    }

    print("\n" + "="*60)
    print("     STUDENT DATA PIPELINE - EXECUTION STARTED")
    print("="*60)
    print(f"\nData directory: {data_dir}")
    print(f"Start time: {start_time}")

    # Step 1: Initialize database schemas
    print("\n" + "-"*50)
    print("STEP 1: Initializing database schemas...")
    print("-"*50)
    init_schemas(db_path)

    # Step 2: Ingest data (Bronze layer)
    print("\n" + "-"*50)
    print("STEP 2: Ingesting data (Bronze layer)")
    print("-"*50)

    # Find and ingest source files
    students_file = data_dir / "students.csv"
    attendance_file = data_dir / "attendance.csv"
    assessments_file = data_dir / "assessments.json"

    if students_file.exists():
        results['ingestion']['students'] = ingest_students(students_file, db_path)
    else:
        print(f"Warning: {students_file} not found")

    if attendance_file.exists():
        results['ingestion']['attendance'] = ingest_attendance(attendance_file, db_path)
    else:
        print(f"Warning: {attendance_file} not found")

    if assessments_file.exists():
        results['ingestion']['assessments'] = ingest_assessments(assessments_file, db_path)
    else:
        print(f"Warning: {assessments_file} not found")

    # Step 3: Transform data (Silver layer)
    print("\n" + "-"*50)
    print("STEP 3: Transforming data (Silver layer)")
    print("-"*50)
    results['transformation'] = run_transformations(db_path)

    # Step 4: Build analytics (Gold layer)
    print("\n" + "-"*50)
    print("STEP 4: Building analytics (Gold layer)")
    print("-"*50)
    results['analytics'] = run_analytics(db_path)

    # Summary
    end_time = datetime.now()
    duration = end_time - start_time
    results['end_time'] = end_time
    results['duration'] = str(duration)

    print("\n" + "="*60)
    print("     PIPELINE EXECUTION COMPLETE")
    print("="*60)
    print(f"\nDuration: {duration}")
    print("\nResults Summary:")
    print(f"  - Bronze layer: {sum(results['ingestion'].values())} total records ingested")
    print(f"  - Silver layer: {sum(results['transformation'].values())} total records transformed")
    print(f"  - Gold layer: {sum(results['analytics'].values())} total records aggregated")

    return results


def main():
    """Main entry point for CLI execution."""
    parser = argparse.ArgumentParser(description='Student Data Pipeline')
    parser.add_argument(
        '--data-dir',
        type=Path,
        default=PROJECT_ROOT / "data" / "raw",
        help='Directory containing source data files'
    )
    parser.add_argument(
        '--db-path',
        type=Path,
        default=None,
        help='Path to DuckDB database file'
    )

    args = parser.parse_args()

    # Check if data directory exists
    if not args.data_dir.exists():
        print(f"Error: Data directory not found: {args.data_dir}")
        print("\nPlease copy the source files to the data/raw directory:")
        print("  - students.csv")
        print("  - attendance.csv")
        print("  - assessments.json")
        sys.exit(1)

    try:
        results = run_pipeline(args.data_dir, args.db_path)
        return 0
    except Exception as e:
        print(f"\nError: Pipeline execution failed!")
        print(f"  {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
