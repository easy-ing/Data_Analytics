# 사용자 행동 로그 기반 콘텐츠 분석 시스템

네이버 Analytics Engineer 인턴 자소서에서 다음 3가지 역량을 한 번에 보여주기 위한 프로젝트입니다.
- 데이터 기반 문제 해결 경험
- SQL 분석 역량
- 데이터 모델링 경험

## 프로젝트 목적
콘텐츠 서비스에서 발생하는 사용자 행동 로그(노출, 클릭, 체류, 반응)를 구조적으로 수집하고,
PostgreSQL 기반 분석 모델로 전환해 KPI를 계산한 뒤,
실제 개선 가능한 인사이트까지 도출하는 end-to-end 분석 시스템을 구현합니다.

## 전체 폴더 구조
```text
naver-content-analytics/
├─ backend/
│  ├─ __init__.py
│  └─ main.py
├─ data/
│  ├─ raw/
│  └─ processed/
├─ sql/
│  ├─ 01_schema.sql
│  └─ 02_kpi_queries.sql
├─ etl/
│  ├─ generate_sample_logs.py
│  ├─ load_to_postgres.py
│  └─ build_mart.py
├─ docs/
│  ├─ folder_roles.md
│  └─ project_flow.md
├─ .env.example
├─ docker-compose.yml
├─ Makefile
├─ requirements.txt
└─ README.md
```

## 폴더 역할
- `backend/`: 이벤트 수집 API와 지표 조회 API
- `data/`: 원천/가공 데이터 저장
- `sql/`: 스키마 및 KPI 쿼리 관리
- `etl/`: 배치 파이프라인(생성, 적재, 마트 빌드)
- `docs/`: 구조/흐름/자소서 연결 문서

## 프로젝트 흐름
1. 데이터 수집
- 샘플 로그 생성 스크립트(`etl/generate_sample_logs.py`) 또는 API(`/events`)로 이벤트 확보

2. 데이터 저장
- 원천 로그를 `data/raw`에 저장
- PostgreSQL `analytics` 스키마의 차원/팩트 테이블로 적재

3. 데이터 분석
- `mart_content_daily` 집계 테이블 생성
- KPI SQL 실행(DAU, CTR, 평균 체류시간, 7일 재방문율, 카테고리별 인기 콘텐츠)

4. 인사이트 도출
- 지표 기반으로 추천 효율, 카테고리별 성과, 리텐션 개선 포인트 정의

## 빠른 실행 가이드
### 1) PostgreSQL 실행
```bash
docker compose up -d
```

### 2) 가상환경 및 패키지 설치
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

### 3) 스키마 생성
```bash
psql -h localhost -p 5432 -U postgres -d analytics -f sql/01_schema.sql
```

### 4) 샘플 로그 생성 및 적재
```bash
python etl/generate_sample_logs.py
python etl/load_to_postgres.py
python etl/build_mart.py
```

### 5) KPI 분석
```bash
psql -h localhost -p 5432 -U postgres -d analytics -f sql/02_kpi_queries.sql
```

### 6) API 실행 (선택)
```bash
uvicorn backend.main:app --reload --port 8000
```
- Health check: `GET /health`
- Event ingest: `POST /events`
- DAU 조회: `GET /insights/dau`

## 자소서 연결 방법
- 데이터 기반 문제 해결 경험: "콘텐츠 소비 저하" 문제를 가정하고, 이벤트 설계부터 KPI 정의/분석/개선안 제시까지 수행
- SQL 분석 역량: CTE, FILTER, 윈도우 함수, NULLIF 등 실무형 SQL로 KPI 설계
- 데이터 모델링 경험: `dim_users`, `dim_content`, `fact_user_events`, `mart_content_daily` 분리 모델 적용
