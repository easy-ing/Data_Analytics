# 실행/검증 런북

## 1) 사전 조건
- Docker 실행 가능
- Python 3.10+
- PostgreSQL 클라이언트(`psql`) 설치

## 2) 실행 절차
```bash
docker compose up -d
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
psql -h localhost -p 5432 -U postgres -d analytics -f sql/03_dw_schema.sql
python etl/generate_dummy_data.py --seed 20260317 --output-dir data/raw
python etl/load_to_postgres.py --raw-dir data/raw
python etl/build_mart.py
```

## 3) 검증 SQL
```sql
-- 팩트 적재 건수
SELECT COUNT(*) AS fact_events_cnt FROM analytics.fact_events;

-- 마트 집계 건수
SELECT COUNT(*) AS mart_rows FROM analytics.mart_content_daily;

-- 최근 7일 DAU
SELECT event_date, COUNT(DISTINCT user_id) AS dau
FROM analytics.fact_events
WHERE event_date >= CURRENT_DATE - INTERVAL '6 days'
GROUP BY event_date
ORDER BY event_date;
```

## 4) 자주 발생하는 문제
- `relation "analytics.fact_events" does not exist`
  - 원인: DW 스키마 미생성
  - 조치: `sql/03_dw_schema.sql` 재실행

- `permission denied for schema analytics`
  - 원인: DB 권한 부족
  - 조치: 스키마/테이블 권한 부여 또는 계정 변경

- `Missing required columns in events.csv`
  - 원인: 입력 CSV 컬럼 누락
  - 조치: `event_id,event_ts,user_id,content_id,session_id,event_type` 컬럼 확인
