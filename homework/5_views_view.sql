CREATE OR REPLACE VIEW views AS
SELECT 
    title,
    views,
    rank,
    CAST(date AS DATE) as date,
    CAST(from_iso8601_timestamp(retrieved_at) AS TIMESTAMP) as retrieved_at
FROM raw_views
ORDER BY date ASC, rank ASC;