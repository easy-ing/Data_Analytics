# 사용자 행동 로그 기반 콘텐츠 분석 시스템

프로젝트 목적
- 사용자 행동 로그를 기반으로 콘텐츠 성과를 분석하는 엔드투엔드 데이터 파이프라인을 구현합니다. 이 프로젝트는 이벤트 생성 → 적재 → 차원/팩트 기반 데이터 모델링 → SQL 기반 KPI 산출 → 간단한 API를 통한 지표 조회까지 전체 흐름을 포함합니다.

세부 목표
- 이벤트 로그 수집 및 표준화 포맷 정의 (예: view, click, like, share)
- PostgreSQL 기반 데이터 웨어하우스(DW) 스키마 설계(Star Schema: dim / fact / mart)
- 더미 데이터 생성 스크립트로 실전 수준의 테스트 데이터 확보(사용자 2,000명 / 콘텐츠 500개 / 이벤트 100,000건)
- ETL 파이프라인(CSV 생성 → PostgreSQL 적재) 자동화 스크립트 제공
- 핵심 지표(KPI) SQL 쿼리 제공: DAU, CTR, 평균 체류시간, 7일 재방문율, 카테고리별 인기콘텐츠 등
- 간단한 REST API(uvicorn)로 엔드포인트 제공: 헬스 체크·이벤트 수집·주요 인사이트 조회

산출물(Outputs)
- DW 스키마 정의 파일: sql/03_dw_schema.sql
- KPI 쿼리 모음: sql/02_kpi_queries.sql, sql/04_advanced_analytics.sql
- 더미 데이터 생성 스크립트: etl/generate_dummy_data.py
- 적재/ETL 스크립트: etl/load_to_postgres.py (및 load_csv_to_postgres.py)
- 간단한 분석 API: backend/main.py
- 문서: docs/* (데이터 딕셔너리, 프로젝트 흐름 등)

성공 기준 / 검증 방법
- 더미 데이터로 KPI 쿼리 실행 시 의미 있는(예상 범위 내) 지표가 도출된다.
- ETL 스크립트를 통해 CSV → PostgreSQL 적재가 자동으로 수행된다.
- mart 테이블(일별 요약) 생성 후 쿼리 응답 시간이 개선되는 것을 확인한다(예: 반복 조회 시).
- 제공된 API로 DAU 등 핵심 지표를 조회할 수 있다.

확장 및 운영 아이디어
- Airflow / Prefect로 배치 오케스트레이션 구성
- dbt로 모델링/테스트 자동화 (uniqueness, not null 등)
- 모니터링 및 알림(데이터 파이프라인 실패 시 슬랙 알림)
- 대시보드 연동 (Looker Studio / Grafana)

## 프로젝트 개요
이 프로젝트는 이벤트 로그를 생성하고(PostgreSQL 적재), 차원/팩트 모델로 분석 가능한 형태로 구성한 뒤, 실무형 SQL로 KPI를 도출합니다.

- 데이터 규모(더미): 사용자 2,000명 / 콘텐츠 500개 / 이벤트 100,000건
- 이벤트 타입: `view`, `click`, `like`, `share`
- 특징: 카테고리별 클릭률/체류시간 분포를 다르게 설계

## 프로젝트 구조
```text
naver-content-analytics/
├─ backend/
│  ├─ __init__.py
│  └─ main.py
├─ data/
│  ├─ raw/
│  └─ processed/
├─ docs/
│  ├─ data_dictionary.md
│  ├─ folder_roles.md
│  └─ project_flow.md
├─ etl/
│  ├─ build_mart.py
│  ├─ generate_dummy_data.py
│  ├─ generate_sample_logs.py
│  ├─ load_csv_to_postgres.py
│  └─ load_to_postgres.py
├─ sql/
│  ├─ 01_schema.sql
│  ├─ 02_kpi_queries.sql
│  ├─ 03_dw_schema.sql
│  └─ 04_advanced_analytics.sql
├─ .env.example
├─ .gitignore
├─ docker-compose.yml
├─ Makefile
├─ requirements.txt
└─ README.md
```

## 데이터 모델 (Star Schema)
- `analytics.dim_users`: 사용자 차원
- `analytics.dim_contents`: 콘텐츠 차원
- `analytics.dim_event_types`: 이벤트 타입 차원
- `analytics.fact_events`: 행동 이벤트 팩트(월별 range partition)
- `analytics.mart_content_daily`: 일별 콘텐츠 KPI 마트

왜 이 구조를 선택했는가:
- 차원/팩트 분리로 지표 정의를 일관되게 유지
- 대용량 이벤트는 파티션 + 인덱스로 조회 성능 확보
- 반복 KPI는 마트 테이블로 사전 집계해 분석 효율 개선

## 실행 방법
### 1) PostgreSQL 실행
```bash
docker compose up -d
```

### 2) 환경 준비
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

### 3) DW 스키마 생성
```bash
psql -h localhost -p 5432 -U postgres -d analytics -f sql/03_dw_schema.sql
```

### 4) 더미 데이터 생성
```bash
python etl/generate_dummy_data.py --seed 20260317 --output-dir data/raw
```

### 5) CSV -> PostgreSQL ETL 적재
```bash
python etl/load_to_postgres.py --raw-dir data/raw
```

`load_to_postgres.py`는 표준 적재 스크립트 `load_csv_to_postgres.py`를 호출하는 단일 진입점입니다.

### 6) 분석 SQL 실행
기본 KPI:
```bash
psql -h localhost -p 5432 -U postgres -d analytics -f sql/02_kpi_queries.sql
```

고급 분석:
```bash
psql -h localhost -p 5432 -U postgres -d analytics -f sql/04_advanced_analytics.sql
```

## 포함된 주요 분석
`sql/04_advanced_analytics.sql` 기준:
- DAU
- CTR
- 평균 체류시간
- 7일 재방문율
- 카테고리별 인기 콘텐츠 TOP 10
- 사용자별 평균 활동량
- 이탈률 추정(7일 + 롤링)

## API
```bash
uvicorn backend.main:app --reload --port 8000
```
- `GET /health`
- `POST /events`
- `GET /insights/dau`
