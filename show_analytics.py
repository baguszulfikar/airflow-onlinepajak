import duckdb
import pandas as pd

# Connect to the warehouse
DB_PATH = "data/warehouse.duckdb"

def get_connection():
    return duckdb.connect(DB_PATH)

def show_class_daily_performance(limit=20):
    """Display class daily performance metrics."""
    con = get_connection()
    query = f"""
        SELECT
            class_id,
            date,
            total_students,
            present_count,
            absent_count,
            ROUND(attendance_rate * 100, 2) as attendance_rate_pct,
            assessment_count,
            ROUND(avg_score, 2) as avg_score
        FROM gold.class_daily_performance
        ORDER BY class_id, date
        LIMIT {limit}
    """
    df = con.sql(query).fetchdf()
    con.close()
    return df

def show_student_performance_summary(limit=20):
    """Display student performance summary."""
    con = get_connection()
    query = f"""
        SELECT *
        FROM gold.student_performance_summary
        LIMIT {limit}
    """
    df = con.sql(query).fetchdf()
    con.close()
    return df

def show_class_statistics():
    """Display aggregated class statistics."""
    con = get_connection()
    query = """
        SELECT
            class_id,
            COUNT(*) as total_days,
            ROUND(AVG(attendance_rate) * 100, 2) as avg_attendance_rate_pct,
            ROUND(AVG(avg_score), 2) as overall_avg_score,
            SUM(present_count) as total_present,
            SUM(absent_count) as total_absent
        FROM gold.class_daily_performance
        GROUP BY class_id
        ORDER BY class_id
    """
    df = con.sql(query).fetchdf()
    con.close()
    return df

def main():
    print("=" * 60)
    print("STUDENT DATA WAREHOUSE - ANALYTICS DASHBOARD")
    print("=" * 60)

    # Class Daily Performance
    print("\n[1] CLASS DAILY PERFORMANCE (Sample)")
    print("-" * 60)
    df_daily = show_class_daily_performance(limit=10)
    print(df_daily.to_string(index=False))

    print("\n" + "=" * 60)
    print("End of Report")
    print("=" * 60)

if __name__ == "__main__":
    main()
