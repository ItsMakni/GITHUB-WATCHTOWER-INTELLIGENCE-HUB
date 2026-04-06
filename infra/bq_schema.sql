-- ############################################################
-- PROJECT WATCHTOWER: BIGQUERY SCHEMA DEFINITIONS
-- Author: Mohamed Makni
-- Description: Central Data Warehouse for GitHub Archive Events
-- ############################################################

-- 1. TECH TRENDS TABLE
-- Stores the raw library extractions from Spark Structured Streaming
CREATE OR REPLACE TABLE `githubleakmonitor-492112.watchtower_db.tech_trends` (
    library_name STRING NOT NULL,
    language STRING,
    repo_name STRING,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(detected_at)
CLUSTER BY library_name, language;

-- 2. DETECTED LEAKS TABLE
-- Stores security flags and high-entropy code signatures
CREATE OR REPLACE TABLE `githubleakmonitor-492112.watchtower_db.detected_leaks` (
    repo_name STRING,
    commit_sha STRING NOT NULL,
    entropy_score FLOAT64,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(detected_at)
CLUSTER BY entropy_score;

-- 3. WEEKLY VIBES TABLE (AI Insights)
-- Stores the Gemini-generated sector analysis
CREATE OR REPLACE TABLE `githubleakmonitor-492112.watchtower_db.weekly_vibes` (
    vibe_date DATE DEFAULT CURRENT_DATE(),
    theme_name STRING,
    summary STRING,
    top_libraries ARRAY<STRING>
)
PARTITION BY vibe_date;

-- ############################################################
-- PERFORMANCE OPTIMIZATIONS
-- ############################################################

-- Create a Search Index on library names to speed up Dashboard filtering
CREATE SEARCH INDEX IF NOT EXISTS idx_library_search
ON `githubleakmonitor-492112.watchtower_db.tech_trends`(library_name);

-- Create a View for the Public Dashboard (Applies Masking at the DB level)
CREATE OR REPLACE VIEW `githubleakmonitor-492112.watchtower_db.v_public_security_stream` AS
SELECT 
    REGEXP_REPLACE(repo_name, r'([^/]{3})[^/]+', r'\1***') AS masked_repo,
    SUBSTR(commit_sha, 1, 8) || '****************' AS masked_sha,
    ROUND(entropy_score, 2) as entropy_score,
    detected_at
FROM `githubleakmonitor-492112.watchtower_db.detected_leaks`
ORDER BY detected_at DESC;