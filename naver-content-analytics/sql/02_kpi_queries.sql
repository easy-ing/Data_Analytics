-- ==========================================================
-- Core KPI SQL (PostgreSQL)
-- Schema: analytics.fact_events, analytics.dim_event_types, analytics.dim_contents
-- ==========================================================

-- 1) DAU
SELECT
    f.event_date,
    COUNT(DISTINCT f.user_id) AS dau
FROM analytics.fact_events f
GROUP BY f.event_date
ORDER BY f.event_date;


-- 2) CTR (impression -> click)
WITH daily AS (
    SELECT
        f.event_date,
        COUNT(*) FILTER (WHERE et.event_type_code = 'impression') AS impressions,
        COUNT(*) FILTER (WHERE et.event_type_code = 'click') AS clicks
    FROM analytics.fact_events f
    JOIN analytics.dim_event_types et
      ON f.event_type_sk = et.event_type_sk
    GROUP BY f.event_date
)
SELECT
    event_date,
    impressions,
    clicks,
    ROUND(clicks::NUMERIC / NULLIF(impressions, 0), 4) AS ctr
FROM daily
ORDER BY event_date;


-- 3) 평균 체류시간 (view_end 기준)
SELECT
    f.event_date,
    ROUND(AVG(NULLIF(f.dwell_seconds, 0))::NUMERIC, 2) AS avg_dwell_seconds
FROM analytics.fact_events f
JOIN analytics.dim_event_types et
  ON f.event_type_sk = et.event_type_sk
WHERE et.event_type_code = 'view_end'
GROUP BY f.event_date
ORDER BY f.event_date;


-- 4) 7일 재방문율
WITH daily_users AS (
  SELECT DISTINCT
    f.event_date,
    f.user_id
  FROM analytics.fact_events f
), revisit AS (
  SELECT
    a.event_date AS base_date,
    a.user_id
  FROM daily_users a
  WHERE EXISTS (
    SELECT 1
    FROM daily_users b
    WHERE b.user_id = a.user_id
      AND b.event_date > a.event_date
      AND b.event_date <= a.event_date + INTERVAL '7 days'
  )
)
SELECT
  d.event_date AS base_date,
  COUNT(DISTINCT d.user_id) AS base_users,
  COUNT(DISTINCT r.user_id) AS revisit_users,
  ROUND(COUNT(DISTINCT r.user_id)::NUMERIC / NULLIF(COUNT(DISTINCT d.user_id), 0), 4) AS revisit_rate_7d
FROM daily_users d
LEFT JOIN revisit r
  ON r.base_date = d.event_date
 AND r.user_id = d.user_id
GROUP BY d.event_date
ORDER BY d.event_date;


-- 5) 카테고리별 인기 콘텐츠 TOP 5
WITH s AS (
  SELECT
    c.content_category,
    c.content_id,
    c.content_title,
    COUNT(*) FILTER (WHERE et.event_type_code = 'click') AS clicks,
    COUNT(*) FILTER (WHERE et.event_type_code IN ('like', 'comment', 'share', 'bookmark')) AS engagements,
    (
      1.0 * COUNT(*) FILTER (WHERE et.event_type_code = 'click')
      + 2.0 * COUNT(*) FILTER (WHERE et.event_type_code = 'like')
      + 3.0 * COUNT(*) FILTER (WHERE et.event_type_code = 'comment')
      + 4.0 * COUNT(*) FILTER (WHERE et.event_type_code = 'share')
      + 1.5 * COUNT(*) FILTER (WHERE et.event_type_code = 'bookmark')
    ) AS popularity_score
  FROM analytics.fact_events f
  JOIN analytics.dim_event_types et
    ON f.event_type_sk = et.event_type_sk
  JOIN analytics.dim_contents c
    ON f.content_sk = c.content_sk
  WHERE f.event_date >= CURRENT_DATE - INTERVAL '29 days'
  GROUP BY c.content_category, c.content_id, c.content_title
), r AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY content_category
      ORDER BY popularity_score DESC, clicks DESC
    ) AS rk
  FROM s
)
SELECT
  content_category,
  content_id,
  content_title,
  clicks,
  engagements,
  popularity_score
FROM r
WHERE rk <= 5
ORDER BY content_category, rk;
