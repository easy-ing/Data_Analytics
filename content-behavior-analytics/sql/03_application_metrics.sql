-- PostgreSQL core application metrics queries

-- [DAU]
-- 비즈니스 의미:
-- 서비스의 일간 활성 사용자 수를 측정해 콘텐츠 소비 트래픽 규모와 성장 추이를 확인한다.
SELECT
    event_date,
    COUNT(DISTINCT user_id) AS dau
FROM analytics.fact_user_events
GROUP BY event_date
ORDER BY event_date;


-- [CTR]
-- 비즈니스 의미:
-- 노출 대비 클릭 전환 효율을 측정해 콘텐츠 추천/배치/썸네일 품질을 평가한다.
WITH daily AS (
    SELECT
        event_date,
        COUNT(*) FILTER (WHERE event_type = 'impression') AS impressions,
        COUNT(*) FILTER (WHERE event_type = 'click') AS clicks
    FROM analytics.fact_user_events
    GROUP BY event_date
)
SELECT
    event_date,
    impressions,
    clicks,
    ROUND(clicks::NUMERIC / NULLIF(impressions, 0), 4) AS ctr
FROM daily
ORDER BY event_date;


-- [평균 체류시간]
-- 비즈니스 의미:
-- 콘텐츠 소비의 질(몰입도)을 측정하는 지표로, 추천 정합성과 콘텐츠 품질 개선의 근거로 사용한다.
SELECT
    event_date,
    ROUND(AVG(NULLIF(dwell_seconds, 0))::NUMERIC, 2) AS avg_dwell_seconds
FROM analytics.fact_user_events
WHERE event_type = 'view_end'
GROUP BY event_date
ORDER BY event_date;


-- [7일 재방문율]
-- 비즈니스 의미:
-- 특정 날짜 활성 유저가 7일 이내 다시 방문하는 비율을 측정해 사용자 리텐션과 재방문 유도 성과를 확인한다.
WITH daily_users AS (
    SELECT DISTINCT event_date, user_id
    FROM analytics.fact_user_events
),
revisit_7d AS (
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
    COUNT(DISTINCT r.user_id) AS revisited_users_7d,
    ROUND(
        COUNT(DISTINCT r.user_id)::NUMERIC / NULLIF(COUNT(DISTINCT d.user_id), 0),
        4
    ) AS revisit_rate_7d
FROM daily_users d
LEFT JOIN revisit_7d r
  ON d.event_date = r.base_date
 AND d.user_id = r.user_id
GROUP BY d.event_date
ORDER BY d.event_date;


-- [카테고리별 인기 콘텐츠]
-- 비즈니스 의미:
-- 카테고리 내에서 성과가 높은 콘텐츠를 식별해 편성/추천 우선순위와 제작 방향을 결정한다.
WITH content_scores AS (
    SELECT
        c.content_category,
        c.content_id,
        c.content_title,
        COUNT(*) FILTER (WHERE e.event_type = 'click') AS clicks,
        COUNT(*) FILTER (WHERE e.event_type = 'view_end') AS views_completed,
        COUNT(*) FILTER (WHERE e.event_type IN ('like', 'comment', 'share', 'bookmark')) AS engagements,
        (
            1.0 * COUNT(*) FILTER (WHERE e.event_type = 'click')
          + 2.0 * COUNT(*) FILTER (WHERE e.event_type = 'like')
          + 3.0 * COUNT(*) FILTER (WHERE e.event_type = 'comment')
          + 4.0 * COUNT(*) FILTER (WHERE e.event_type = 'share')
          + 1.5 * COUNT(*) FILTER (WHERE e.event_type = 'bookmark')
        ) AS popularity_score
    FROM analytics.fact_user_events e
    JOIN analytics.dim_content c
      ON e.content_id = c.content_id
    WHERE e.event_date >= CURRENT_DATE - INTERVAL '29 days'
    GROUP BY c.content_category, c.content_id, c.content_title
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY content_category
            ORDER BY popularity_score DESC, views_completed DESC, clicks DESC
        ) AS rnk
    FROM content_scores
)
SELECT
    content_category,
    content_id,
    content_title,
    clicks,
    views_completed,
    engagements,
    popularity_score
FROM ranked
WHERE rnk <= 5
ORDER BY content_category, rnk;
