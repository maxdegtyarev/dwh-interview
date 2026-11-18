-- INTERVIEW PROBLEM: User Conversion Behavior
--
-- TASK:
--   Identify our top spending users over the last 30 days.
--   For each user, we need their total spend and the exact number of
--   unique items they viewed before purchasing.
--
-- A junior data engineer submitted the query below to run daily in our pipeline.
-- Review it: what is wrong, and how would you fix / productionize it?
-- Also: how would you cluster the table, and how would you design this
-- model given ~500GB/day of ingestion?
--
-- Table: {{ source('stg_events', 'mobile_telemetry') }}  (partitioned by event_date, cluster by ?)

WITH recent_events AS (
    SELECT user_id, event_id
    FROM {{ source('stg_events', 'mobile_telemetry') }}
    WHERE FORMAT_TIMESTAMP(event_timestamp, '%Y-%m-%d') >= '2026-07-27'
),
purchases AS (
    SELECT
        user_id,
        SUM(CAST(JSON_EXTRACT_SCALAR(event_properties, '$.price') AS FLOAT64)) AS total_spend
    FROM recent_events
    WHERE event_name = 'purchase'
    GROUP BY user_id
),
item_views AS (
    SELECT
        user_id,
        item_id,
        COUNT(DISTINCT JSON_EXTRACT_SCALAR(event_properties, '$.item_id')) AS distinct_items_viewed
    FROM recent_events
    WHERE event_name = 'view_item'
    GROUP BY user_id
),
new_data_source_aggregate AS (
    SELECT * FROM {{ source('stg_events', 'extra_data_source') }} -- has user_id and a bunch of other columns
)

SELECT
    p.user_id, -- gets duplicates
    p.total_spend,
    v.distinct_items_viewed
FROM purchases p
JOIN item_views v ON p.user_id = v.user_id
LEFT JOIN new_data_source_aggregate USING (user_id)
ORDER BY p.total_spend DESC
