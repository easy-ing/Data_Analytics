# 프로젝트 흐름

## 1) 데이터 준비
- `etl/generate_dummy_data.py`로 샘플 CSV(`data/raw/users.csv`, `contents.csv`, `events.csv`)를 생성한다.
- 이미 보유한 원천 데이터가 있으면 동일 스키마로 `data/raw/`에 배치한다.

## 2) DW 스키마 구성
- `sql/03_dw_schema.sql` 실행으로 스타 스키마를 생성한다.
- 핵심 테이블: `dim_users`, `dim_contents`, `dim_event_types`, `fact_events`, `mart_content_daily`.

## 3) ETL 적재
- `etl/load_to_postgres.py`(= `load_csv_to_postgres.py` 진입점) 실행.
- 처리 순서:
  1. CSV 로드/전처리
  2. `dim_users`, `dim_contents` 업서트
  3. `dim_event_types` 보정
  4. 월 단위 `fact_events` 파티션 자동 생성
  5. `fact_events` 적재

## 4) 마트 집계
- `etl/build_mart.py` 실행으로 `mart_content_daily`를 upsert한다.
- 산출 지표: 노출/클릭/유니크유저/완주/참여지표/CTR/Engagement Rate.

## 5) 분석
- 기본 KPI: `sql/02_kpi_queries.sql`
- 확장 분석: `sql/04_advanced_analytics.sql`

## 6) API 활용(선택)
- `backend/main.py`의 `/events`로 단건 이벤트를 수집할 수 있다.
- `/insights/dau`에서 최근 30일 DAU 조회가 가능하다.
