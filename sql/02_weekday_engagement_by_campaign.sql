WITH events AS (
  SELECT
    user_pseudo_id,
    TIMESTAMP_MICROS(event_timestamp) AS event_time,
    LAG(TIMESTAMP_MICROS(event_timestamp)) OVER(
      PARTITION BY user_pseudo_id
      ORDER BY event_timestamp
    ) AS prev_event_time,
    campaign
  FROM `tc-da-1.turing_data_analytics.raw_events`
),

gaps AS (
  SELECT *,
         TIMESTAMP_DIFF(event_time, prev_event_time, MINUTE) AS gap_minutes
  FROM events
),

session AS (
  SELECT *,
         IF(prev_event_time IS NULL OR gap_minutes > 30, 1, 0) AS is_new_session
  FROM gaps
),

session_count AS (
  SELECT *,
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
    CASE
      WHEN campaign = '(data deleted)' THEN 'Unattributed Paid Campaigns'
      ELSE campaign
    END AS campaign_name,
    event_time
  FROM session_count
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY user_pseudo_id, session_id
    ORDER BY (campaign IS NULL), event_time ASC
  ) = 1
),

paid_only AS (
  SELECT
    user_pseudo_id,
    session_id,
    event_time,
    campaign_name
  FROM session_campaign
  WHERE campaign_name IN (
    'Unattributed Paid Campaigns',
    'BlackFriday_V1','BlackFriday_V2',
    'Data Share Promo',
    'Holiday_V1','Holiday_V2',
    'NewYear_V1','NewYear_V2'
  )
),
session_summary AS (
  SELECT
    s.user_pseudo_id,
    s.session_id,
    MIN(s.event_time) AS session_start,
    MAX(s.event_time) AS session_end,
    TIMESTAMP_DIFF(MAX(s.event_time), MIN(s.event_time), MINUTE) AS session_duration,
    p.campaign_name
  FROM session_count s
  JOIN paid_only p
    ON s.user_pseudo_id = p.user_pseudo_id
   AND s.session_id = p.session_id
  GROUP BY s.user_pseudo_id, s.session_id, p.campaign_name
),

session_analysis AS (
  SELECT
    campaign_name,
    FORMAT_DATE('%A', DATE(session_start)) AS weekday,
    session_duration
  FROM session_summary
)

SELECT
  weekday,
  campaign_name,
  COUNT(*) AS total_sessions,
  ROUND(AVG(session_duration), 2) AS avg_session_duration,
  ROUND(MAX(session_duration), 2) AS max_session_duration,
  ROUND(MIN(session_duration), 2) AS min_session_duration
FROM session_analysis
GROUP BY weekday, campaign_name
ORDER BY 
  CASE weekday
    WHEN 'Monday' THEN 1
    WHEN 'Tuesday' THEN 2
    WHEN 'Wednesday' THEN 3
    WHEN 'Thursday' THEN 4
    WHEN 'Friday' THEN 5
    WHEN 'Saturday' THEN 6
    WHEN 'Sunday' THEN 7
  END,
  campaign_name
