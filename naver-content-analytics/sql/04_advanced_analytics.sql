-- ==========================================================
-- Advanced Analytics SQL (PostgreSQL)
-- Schema: analytics.fact_events, analytics.dim_users, analytics.dim_contents, analytics.dim_event_types
-- ==========================================================

-- 1) DAU (일간 활성 사용자 수)
-- 분석 목적: 일자별 사용자 활성 규모를 파악해 트래픽 변화와 성장/하락 추세를 모니터링한다.
SELECT
    f.event_date,
    COUNT(DISTINCT f.user_id) AS dau
FROM analytics.fact_events f
GROUP BY f.event_date
ORDER BY f.event_date;


-- 2) CTR (클릭률)
-- 분석 목적: 노출(view) 대비 클릭(click) 전환 효율을 측정해 추천/노출 품질을 평가한다.
WITH daily AS (
    SELECT
        f.event_date,
        COUNT(*) FILTER (WHERE et.event_type_code = 'view') AS views,
        COUNT(*) FILTER (WHERE et.event_type_code = 'click') AS clicks
    FROM analytics.fact_events f
    JOIN analytics.dim_event_types et
      ON f.event_type_sk = et.event_type_sk
    GROUP BY f.event_date
)
SELECT
    event_date,
    views,
    clicks,
    ROUND(clicks::NUMERIC / NULLIF(views, 0), 4) AS ctr
FROM daily
ORDER BY event_date;


-- 3) 평균 체류시간
-- 분석 목적: 콘텐츠 소비 품질(몰입도)을 측정해 콘텐츠/추천 전략 개선 우선순위를 결정한다.
SELECT
    f.event_date,
    ROUND(AVG(NULLIF(f.dwell_seconds, 0))::NUMERIC, 2) AS avg_dwell_seconds
FROM analytics.fact_events f
JOIN analytics.dim_event_types et
  ON f.event_type_sk = et.event_type_sk
WHERE et.event_type_code = 'view'
GROUP BY f.event_date
ORDER BY f.event_date;


-- 4) 7일 재방문율
-- 분석 목적: 특정 날짜 활성 사용자 중 7일 내 재방문한 비율을 계산해 리텐션 수준을 진단한다.
WITH daily_active AS (
    SELECT DISTINCT
        f.event_date,
        f.user_id
    FROM analytics.fact_events f
),
revisit_7d AS (
    SELECT
        a.event_date AS base_date,
        a.user_id
    FROM daily_active a
    WHERE EXISTS (
        SELECT 1
        FROM daily_active b
        WHERE b.user_id = a.user_id
          AND b.event_date > a.event_date
          AND b.event_date <= a.event_date + INTERVAL '7 days'
    )
)
SELECT
    a.event_date AS base_date,
    COUNT(DISTINCT a.user_id) AS base_active_users,
    COUNT(DISTINCT r.user_id) AS revisited_users_7d,
    ROUND(
        COUNT(DISTINCT r.user_id)::NUMERIC / NULLIF(COUNT(DISTINCT a.user_id), 0),
        4
    ) AS revisit_rate_7d
FROM daily_active a
LEFT JOIN revisit_7d r
  ON a.event_date = r.base_date
 AND a.user_id = r.user_id
GROUP BY a.event_date
ORDER BY a.event_date;


-- 5) 카테고리별 인기 콘텐츠 TOP 10
-- 분석 목적: 카테고리 내 상위 성과 콘텐츠를 식별해 편성/추천 우선순위 의사결정에 활용한다.
WITH content_metrics AS (
    SELECT
        c.content_category,
        c.content_id,
        c.content_title,
        COUNT(*) FILTER (WHERE et.event_type_code = 'view') AS views,
        COUNT(*) FILTER (WHERE et.event_type_code = 'click') AS clicks,
        COUNT(*) FILTER (WHERE et.event_type_code = 'like') AS likes,
        COUNT(*) FILTER (WHERE et.event_type_code = 'share') AS shares,
        ROUND(AVG(NULLIF(f.dwell_seconds, 0))::NUMERIC, 2) AS avg_dwell_seconds,
        (
            1.0 * COUNT(*) FILTER (WHERE et.event_type_code = 'view')
          + 2.0 * COUNT(*) FILTER (WHERE et.event_type_code = 'click')
          + 3.0 * COUNT(*) FILTER (WHERE et.event_type_code = 'like')
          + 4.0 * COUNT(*) FILTER (WHERE et.event_type_code = 'share')
        ) AS popularity_score
    FROM analytics.fact_events f
    JOIN analytics.dim_event_types et
      ON f.event_type_sk = et.event_type_sk
    JOIN analytics.dim_contents c
      ON f.content_sk = c.content_sk
    WHERE f.event_date >= CURRENT_DATE - INTERVAL '29 days'
    GROUP BY c.content_category, c.content_id, c.content_title
),
ranked AS (
    SELECT
        content_category,
        content_id,
        content_title,
        views,
        clicks,
        likes,
        shares,
        avg_dwell_seconds,
        popularity_score,
        ROW_NUMBER() OVER (
            PARTITION BY content_category
            ORDER BY popularity_score DESC, clicks DESC, views DESC
        ) AS rank_in_category
    FROM content_metrics
)
SELECT
    content_category,
    rank_in_category,
    content_id,
    content_title,
    views,
    clicks,
    likes,
    shares,
    avg_dwell_seconds,
    popularity_score
FROM ranked
WHERE rank_in_category <= 10
ORDER BY content_category, rank_in_category;


-- 6) 사용자별 평균 활동량
-- 분석 목적: 사용자별 활동 강도를 정량화해 핵심 사용자군(헤비/라이트)을 분류하고 CRM 전략에 반영한다.
WITH user_daily AS (
    SELECT
        f.user_id,
        f.event_date,
        COUNT(*) AS events_per_day
    FROM analytics.fact_events f
    GROUP BY f.user_id, f.event_date
),
user_activity AS (
    SELECT
        u.user_id,
        COALESCE(u.region, 'unknown') AS region,
        COALESCE(u.acquisition_channel, 'unknown') AS acquisition_channel,
        COUNT(*) AS active_days,
        SUM(ud.events_per_day) AS total_events,
        ROUND(AVG(ud.events_per_day)::NUMERIC, 2) AS avg_events_per_active_day
    FROM user_daily ud
    JOIN analytics.dim_users u
      ON ud.user_id = u.user_id
    GROUP BY u.user_id, u.region, u.acquisition_channel
)
SELECT
    user_id,
    region,
    acquisition_channel,
    active_days,
    total_events,
    avg_events_per_active_day,
    NTILE(10) OVER (ORDER BY avg_events_per_active_day DESC) AS activity_decile
FROM user_activity
ORDER BY avg_events_per_active_day DESC, total_events DESC;


-- 7) 이탈률 추정 (7일 롤링 기준)
-- 분석 목적: 기준일 이전 7일 활성 사용자 중 이후 7일 미재방문 사용자의 비율을 계산해 이탈 위험도를 추정한다.
WITH daily_active AS (
    SELECT DISTINCT
        f.event_date,
        f.user_id
    FROM analytics.fact_events f
),
base_users AS (
    SELECT
        d.event_date AS base_date,
        d.user_id
    FROM daily_active d
),
retained AS (
    SELECT
        b.base_date,
        b.user_id
    FROM base_users b
    WHERE EXISTS (
        SELECT 1
        FROM daily_active d2
        WHERE d2.user_id = b.user_id
          AND d2.event_date > b.base_date
          AND d2.event_date <= b.base_date + INTERVAL '7 days'
    )
),
daily_churn AS (
    SELECT
        b.base_date,
        COUNT(DISTINCT b.user_id) AS base_users,
        COUNT(DISTINCT r.user_id) AS retained_users_7d,
        COUNT(DISTINCT b.user_id) - COUNT(DISTINCT r.user_id) AS churned_users_7d,
        ROUND(
            (COUNT(DISTINCT b.user_id) - COUNT(DISTINCT r.user_id))::NUMERIC
            / NULLIF(COUNT(DISTINCT b.user_id), 0),
            4
        ) AS churn_rate_7d
    FROM base_users b
    LEFT JOIN retained r
      ON b.base_date = r.base_date
     AND b.user_id = r.user_id
    GROUP BY b.base_date
)
SELECT
    base_date,
    base_users,
    retained_users_7d,
    churned_users_7d,
    churn_rate_7d,
    ROUND(
        AVG(churn_rate_7d) OVER (
            ORDER BY base_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ),
        4
    ) AS churn_rate_7d_rolling_avg
FROM daily_churn
ORDER BY base_date;
