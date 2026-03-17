-- DAU: 일간 활성 사용자 규모를 측정해 트래픽 변화를 모니터링
SELECT event_date, COUNT(DISTINCT user_id) AS dau
FROM analytics.fact_user_events
GROUP BY event_date
ORDER BY event_date;

-- CTR: 노출 대비 클릭 전환율을 측정해 추천/배치 품질을 평가
WITH d AS (
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
FROM d
ORDER BY event_date;

-- 평균 체류시간: 콘텐츠 몰입도를 파악해 품질 개선 우선순위를 설정
SELECT
  event_date,
  ROUND(AVG(NULLIF(dwell_seconds, 0))::NUMERIC, 2) AS avg_dwell_seconds
FROM analytics.fact_user_events
WHERE event_type = 'view_end'
GROUP BY event_date
ORDER BY event_date;

-- 7일 재방문율: 기준일 활성 사용자가 7일 내 재방문하는 비율로 리텐션 확인
WITH daily_users AS (
  SELECT DISTINCT event_date, user_id
  FROM analytics.fact_user_events
), revisit AS (
  SELECT a.event_date AS base_date, a.user_id
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
LEFT JOIN revisit r ON r.base_date = d.event_date AND r.user_id = d.user_id
GROUP BY d.event_date
ORDER BY d.event_date;

-- 카테고리별 인기 콘텐츠: 카테고리별 상위 콘텐츠를 뽑아 편성/추천 전략에 활용
WITH s AS (
  SELECT
    c.content_category,
    c.content_id,
    c.content_title,
    COUNT(*) FILTER (WHERE e.event_type = 'click') AS clicks,
    COUNT(*) FILTER (WHERE e.event_type IN ('like', 'comment', 'share', 'bookmark')) AS engagements,
    (1.0 * COUNT(*) FILTER (WHERE e.event_type = 'click')
     + 2.0 * COUNT(*) FILTER (WHERE e.event_type = 'like')
     + 3.0 * COUNT(*) FILTER (WHERE e.event_type = 'comment')
     + 4.0 * COUNT(*) FILTER (WHERE e.event_type = 'share')
     + 1.5 * COUNT(*) FILTER (WHERE e.event_type = 'bookmark')) AS popularity_score
  FROM analytics.fact_user_events e
  JOIN analytics.dim_content c ON c.content_id = e.content_id
  WHERE e.event_date >= CURRENT_DATE - INTERVAL '29 days'
  GROUP BY c.content_category, c.content_id, c.content_title
), r AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY content_category ORDER BY popularity_score DESC, clicks DESC) AS rk
  FROM s
)
SELECT content_category, content_id, content_title, clicks, engagements, popularity_score
FROM r
WHERE rk <= 5
ORDER BY content_category, rk;
