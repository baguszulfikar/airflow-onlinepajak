-- =============================================================================
-- CLASS DAILY PERFORMANCE TABLE
-- Analytics-ready table for class performance reporting
-- =============================================================================

-- DDL: Create Table
CREATE TABLE IF NOT EXISTS gold.class_daily_performance (
    -- Primary identification
    class_id                    VARCHAR         NOT NULL,   -- Unique class identifier
    date                        DATE            NOT NULL,   -- Date of the performance record

    -- Student counts
    total_students              INTEGER         NOT NULL,   -- Total enrolled students in class

    -- Attendance metrics
    students_with_attendance    INTEGER         NOT NULL,   -- Students with attendance records
    present_count               INTEGER         NOT NULL,   -- Count of students marked present
    absent_count                INTEGER         NOT NULL,   -- Count of students marked absent
    attendance_rate             DECIMAL(5,2)    NOT NULL,   -- present_count / students_with_attendance

    -- Assessment metrics
    students_with_assessment    INTEGER         NOT NULL,   -- Distinct students with assessments
    assessment_count            INTEGER         NOT NULL,   -- Total assessments taken
    avg_score                   DECIMAL(5,2)    NOT NULL,   -- Average score percentage

    -- Metadata
    _created_at                 TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    PRIMARY KEY (class_id, date)
);

-- =============================================================================
-- EXAMPLE DATA
-- =============================================================================

-- Sample insert matching the required schema
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
    avg_score
) VALUES (
    'CLASS-01',         -- class_id
    '2025-01-10',       -- date
    30,                 -- total_students
    28,                 -- students_with_attendance
    25,                 -- present_count
    3,                  -- absent_count
    0.89,               -- attendance_rate (25/28 = 0.893)
    18,                 -- students_with_assessment
    22,                 -- assessment_count
    74.5                -- avg_score
);

-- =============================================================================
-- SAMPLE QUERIES
-- =============================================================================

-- Query 1: Get daily performance for a specific class
SELECT
    class_id,
    date,
    total_students,
    students_with_attendance,
    present_count,
    absent_count,
    ROUND(attendance_rate * 100, 1) || '%' as attendance_pct,
    students_with_assessment,
    assessment_count,
    avg_score
FROM gold.class_daily_performance
WHERE class_id = 'CLASS-01'
ORDER BY date DESC;

-- Query 2: Weekly attendance summary by class
SELECT
    class_id,
    DATE_TRUNC('week', date) as week_start,
    SUM(present_count) as total_present,
    SUM(absent_count) as total_absent,
    ROUND(AVG(attendance_rate), 2) as avg_attendance_rate
FROM gold.class_daily_performance
GROUP BY class_id, DATE_TRUNC('week', date)
ORDER BY class_id, week_start;

-- Query 3: Classes with low attendance rate (< 80%)
SELECT
    class_id,
    date,
    attendance_rate,
    present_count,
    absent_count
FROM gold.class_daily_performance
WHERE attendance_rate < 0.80
ORDER BY attendance_rate ASC;

-- Query 4: Classes with highest average scores
SELECT
    class_id,
    ROUND(AVG(avg_score), 2) as overall_avg_score,
    COUNT(DISTINCT date) as days_with_assessments,
    SUM(assessment_count) as total_assessments
FROM gold.class_daily_performance
WHERE assessment_count > 0
GROUP BY class_id
ORDER BY overall_avg_score DESC;

-- Query 5: Daily trend report
SELECT
    date,
    COUNT(DISTINCT class_id) as active_classes,
    SUM(present_count) as total_present,
    SUM(absent_count) as total_absent,
    ROUND(AVG(attendance_rate), 2) as avg_attendance,
    ROUND(AVG(avg_score), 2) as avg_score
FROM gold.class_daily_performance
GROUP BY date
ORDER BY date;
