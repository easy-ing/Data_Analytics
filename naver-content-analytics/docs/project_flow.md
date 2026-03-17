# 프로젝트 흐름

1. 데이터 수집
- `backend/main.py`의 `/events` API 또는 `etl/generate_sample_logs.py`를 통해 사용자 행동 로그를 생성/수집한다.

2. 데이터 저장
- 원천 데이터는 `data/raw/*.csv`로 저장한다.
- PostgreSQL의 `analytics.fact_user_events`와 차원 테이블(`dim_users`, `dim_content`)로 적재한다.

3. 데이터 분석
- `etl/build_mart.py`를 통해 `mart_content_daily`를 생성한다.
- `sql/02_kpi_queries.sql`로 DAU, CTR, 평균 체류시간, 7일 재방문율, 카테고리별 인기 콘텐츠를 분석한다.

4. 인사이트 도출
- 지표 결과를 기반으로 추천 품질, 콘텐츠 몰입도, 카테고리 전략 개선 인사이트를 도출한다.
- 자소서에서는 "문제 정의 -> 지표 설계 -> 모델링 -> 개선 제안" 흐름으로 연결한다.
