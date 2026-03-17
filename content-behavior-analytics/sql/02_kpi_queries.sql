-- 10 core KPI queries for content analytics

-- Q1) Daily DAU
SELECT
    event_date,
    COUNT(DISTINCT user_id) AS dau
FROM analytics.fact_user_events
GROUP BY event_date
ORDER BY event_date;

-- Q2) Daily impressions / clicks / CTR
SELECT
    event_date,
    SUM(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END) AS impressions,
    SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS clicks,
    ROUND(
        SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(SUM(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END), 0),
        4
    ) AS ctr
FROM analytics.fact_user_events
GROUP BY event_date
ORDER BY event_date;

-- Q3) Top 10 content by unique viewers (last 7 days)
SELECT
    e.content_id,
    c.content_title,
    c.content_category,
    COUNT(DISTINCT e.user_id) AS uv
FROM analytics.fact_user_events e
JOIN analytics.dim_content c ON e.content_id = c.content_id
WHERE e.event_date >= CURRENT_DATE - INTERVAL '6 days'
  AND e.event_type IN ('view_start', 'view_end', 'click')
GROUP BY e.content_id, c.content_title, c.content_category
ORDER BY uv DESC
LIMIT 10;

-- Q4) Category-level engagement rate
SELECT
    c.content_category,
    SUM(CASE WHEN e.event_type IN ('like', 'comment', 'share', 'bookmark') THEN 1 ELSE 0 END) AS engagements,
    SUM(CASE WHEN e.event_type = 'click' THEN 1 ELSE 0 END) AS clicks,
    ROUND(
        SUM(CASE WHEN e.event_type IN ('like', 'comment', 'share', 'bookmark') THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(SUM(CASE WHEN e.event_type = 'click' THEN 1 ELSE 0 END), 0),
        4
    ) AS engagement_rate
FROM analytics.fact_user_events e
JOIN analytics.dim_content c ON e.content_id = c.content_id
GROUP BY c.content_category
ORDER BY engagement_rate DESC NULLS LAST;

-- Q5) Average dwell time by content type
SELECT
    c.content_type,
    ROUND(AVG(NULLIF(e.dwell_seconds, 0))::NUMERIC, 2) AS avg_dwell_seconds
FROM analytics.fact_user_events e
JOIN analytics.dim_content c ON e.content_id = c.content_id
WHERE e.event_type = 'view_end'
GROUP BY c.content_type
ORDER BY avg_dwell_seconds DESC;

-- Q6) Funnel (impression -> click -> view_end)
WITH funnel AS (
    SELECT
        event_date,
        COUNT(*) FILTER (WHERE event_type = 'impression') AS impression_cnt,
        COUNT(*) FILTER (WHERE event_type = 'click') AS click_cnt,
        COUNT(*) FILTER (WHERE event_type = 'view_end') AS view_end_cnt
    FROM analytics.fact_user_events
    GROUP BY event_date
)
SELECT
    event_date,
    impression_cnt,
    click_cnt,
    view_end_cnt,
    ROUND(click_cnt::NUMERIC / NULLIF(impression_cnt, 0), 4) AS click_rate,
    ROUND(view_end_cnt::NUMERIC / NULLIF(click_cnt, 0), 4) AS completion_rate
FROM funnel
ORDER BY event_date;

-- Q7) 신규/기존 유저 비율(일별)
WITH first_seen AS (
    SELECT user_id, MIN(event_date) AS first_date
    FROM analytics.fact_user_events
    GROUP BY user_id
)
SELECT
    e.event_date,
    COUNT(DISTINCT CASE WHEN f.first_date = e.event_date THEN e.user_id END) AS new_users,
    COUNT(DISTINCT CASE WHEN f.first_date < e.event_date THEN e.user_id END) AS returning_users
FROM analytics.fact_user_events e
JOIN first_seen f ON e.user_id = f.user_id
GROUP BY e.event_date
ORDER BY e.event_date;

-- Q8) Referrer performance (CTR)
WITH ref_stats AS (
    SELECT
        referrer,
        COUNT(*) FILTER (WHERE event_type = 'impression') AS impressions,
        COUNT(*) FILTER (WHERE event_type = 'click') AS clicks
    FROM analytics.fact_user_events
    GROUP BY referrer
)
SELECT
    referrer,
    impressions,
    clicks,
    ROUND(clicks::NUMERIC / NULLIF(impressions, 0), 4) AS ctr
FROM ref_stats
ORDER BY ctr DESC NULLS LAST;

-- Q9) Cohort retention (D+1) by signup week
WITH base AS (
    SELECT
        u.user_id,
        DATE_TRUNC('week', u.signup_at)::DATE AS cohort_week,
        MIN(e.event_date) AS first_active_date
    FROM analytics.dim_users u
    JOIN analytics.fact_user_events e ON u.user_id = e.user_id
    GROUP BY u.user_id, DATE_TRUNC('week', u.signup_at)::DATE
),
d1 AS (
    SELECT DISTINCT b.user_id, b.cohort_week
    FROM base b
    JOIN analytics.fact_user_events e
      ON e.user_id = b.user_id
     AND e.event_date = b.first_active_date + INTERVAL '1 day'
)
SELECT
    b.cohort_week,
    COUNT(DISTINCT b.user_id) AS cohort_size,
    COUNT(DISTINCT d.user_id) AS d1_retained,
    ROUND(COUNT(DISTINCT d.user_id)::NUMERIC / NULLIF(COUNT(DISTINCT b.user_id), 0), 4) AS d1_retention
FROM base b
LEFT JOIN d1 d
  ON b.user_id = d.user_id
 AND b.cohort_week = d.cohort_week
GROUP BY b.cohort_week
ORDER BY b.cohort_week;

-- Q10) 콘텐츠 랭킹 점수 (가중치 기반)
SELECT
    c.content_id,
    c.content_title,
    c.content_category,
    (
        1.0 * COUNT(*) FILTER (WHERE e.event_type = 'click')
      + 2.0 * COUNT(*) FILTER (WHERE e.event_type = 'like')
      + 3.0 * COUNT(*) FILTER (WHERE e.event_type = 'comment')
      + 4.0 * COUNT(*) FILTER (WHERE e.event_type = 'share')
      + 1.5 * COUNT(*) FILTER (WHERE e.event_type = 'bookmark')
    ) AS ranking_score
FROM analytics.fact_user_events e
JOIN analytics.dim_content c ON e.content_id = c.content_id
WHERE e.event_date >= CURRENT_DATE - INTERVAL '6 days'
GROUP BY c.content_id, c.content_title, c.content_category
ORDER BY ranking_score DESC
LIMIT 20;
