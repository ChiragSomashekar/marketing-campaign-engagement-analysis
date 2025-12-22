WITH events AS (
  SELECT
    user_pseudo_id,
    event_name,
    TIMESTAMP_MICROS(event_timestamp) AS event_time,
    LAG(TIMESTAMP_MICROS(event_timestamp)) OVER(
      PARTITION BY user_pseudo_id
      ORDER BY event_timestamp
    ) AS prev_event_time,
    campaign
  FROM `tc-da-1.turing_data_analytics.raw_events`
),

gaps AS (
  SELECT
    *,
    TIMESTAMP_DIFF(event_time, prev_event_time, MINUTE) AS gap_minutes
  FROM events
),

session AS (
  SELECT
    *,
    IF(prev_event_time IS NULL OR gap_minutes > 30, 1, 0) AS is_new_session
  FROM gaps
),

session_count AS (
  SELECT
    *,
    SUM(is_new_session) OVER (
      PARTITION BY user_pseudo_id
      ORDER BY event_time
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS session_id
  FROM session
),

session_campaign AS (
  SELECT
    user_pseudo_id,
    session_id,
    campaign AS landing_campaign
  FROM session_count
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY user_pseudo_id, session_id
    ORDER BY (campaign IS NULL), event_time ASC
  ) = 1
),

sessions_purchases AS(
  SELECT
    user_pseudo_id,
    session_id,
    MIN(event_time) AS session_start,
    MAX(event_time) AS session_end,
    TIMESTAMP_DIFF(MAX(event_time), MIN(event_time), MINUTE) AS session_duration,
    MAX(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS has_purchase
    FROM session_count
    GROUP BY user_pseudo_id, session_id
),

bucketed AS (
  SELECT
    CASE
      WHEN session_duration < 1 THEN '<1 min'
      WHEN session_duration BETWEEN 1 AND 5 THEN '1–5 min'
      WHEN session_duration BETWEEN 6 AND 15 THEN '6–15 min'
      WHEN session_duration BETWEEN 16 AND 30 THEN '16–30 min'
      ELSE '>30 min'
    END AS duration_bucket,
    1 AS sessions,
    has_purchase AS purchases
  FROM sessions_purchases
),

totals AS (
  SELECT
    SUM(sessions) AS total_sessions,
    SUM(purchases) AS total_purchases
  FROM bucketed
)

SELECT 
  CASE
    WHEN session_duration < 1 THEN '<1 min'
    WHEN session_duration BETWEEN 1 AND 5 THEN '1–5 min'
    WHEN session_duration BETWEEN 6 AND 15 THEN '6–15 min'
    WHEN session_duration BETWEEN 16 AND 30 THEN '16–30 min'
    ELSE '>30 min'
  END AS duration_bucket,
  COUNT(*) AS sessions,
  SUM(has_purchase) AS purchases
FROM sessions_purchases
GROUP BY duration_bucket
ORDER BY duration_bucket
