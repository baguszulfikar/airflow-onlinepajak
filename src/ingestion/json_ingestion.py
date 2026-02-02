"""
JSON Ingestion Module
Handles ingestion of JSON files (assessments.json) into the Bronze layer.
"""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Optional

from ..utils.db import get_connection


def ingest_assessments(
    file_path: Path,
    db_path: Optional[Path] = None,
    source_name: Optional[str] = None
) -> int:
    """
    Ingest assessments data from JSON into bronze.assessments table.

    Strategy: Incremental Load with Append
    - Parses JSON array
    - Flattens nested structure (if any)
    - Appends new records with deduplication by assessment_id
    - Validates score ranges

    Args:
        file_path: Path to assessments.json
        db_path: Optional path to DuckDB database
        source_name: Optional source identifier

    Returns:
        Number of new records ingested
    """
    print(f"Ingesting assessments from: {file_path}")

    # Read and parse JSON
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Handle both array and object formats
    if isinstance(data, dict):
        # If it's a dict, look for common keys
        if 'data' in data:
            records = data['data']
        elif 'assessments' in data:
            records = data['assessments']
        else:
            records = [data]
    else:
        records = data

    # Convert to DataFrame
    df = pd.DataFrame(records)

    # Data validation
    required_columns = ['assessment_id', 'student_id', 'subject', 'score',
                        'max_score', 'assessment_date', 'created_at']
    missing_cols = set(required_columns) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Type conversions
    df['assessment_id'] = df['assessment_id'].astype(str)
    df['student_id'] = df['student_id'].astype(str)
    df['subject'] = df['subject'].astype(str)
    df['score'] = df['score'].astype(int)
    df['max_score'] = df['max_score'].astype(int)
    df['assessment_date'] = pd.to_datetime(df['assessment_date'])
    df['created_at'] = pd.to_datetime(df['created_at'])

    # Validate score ranges
    invalid_scores = df[df['score'] > df['max_score']]
    if not invalid_scores.empty:
        print(f"Warning: {len(invalid_scores)} records with score > max_score")

    negative_scores = df[df['score'] < 0]
    if not negative_scores.empty:
        print(f"Warning: {len(negative_scores)} records with negative scores")

    # Add metadata columns
    df['_ingested_at'] = datetime.now()
    df['_source_file'] = source_name or file_path.name

    # Insert with deduplication
    with get_connection(db_path) as conn:
        conn.register('assessments_staging', df)

        # Get count of existing records
        existing_count = conn.execute(
            "SELECT COUNT(*) FROM bronze.assessments"
        ).fetchone()[0]

        # Insert only new records
        conn.execute("""
            INSERT OR IGNORE INTO bronze.assessments
            SELECT
                assessment_id,
                student_id,
                subject,
                score,
                max_score,
                assessment_date,
                created_at,
                _ingested_at,
                _source_file
            FROM assessments_staging
            WHERE assessment_id NOT IN (SELECT assessment_id FROM bronze.assessments)
        """)

        new_count = conn.execute(
            "SELECT COUNT(*) FROM bronze.assessments"
        ).fetchone()[0]

    records_added = new_count - existing_count
    print(f"Successfully ingested {records_added} new assessment records (total: {new_count})")
    return records_added
