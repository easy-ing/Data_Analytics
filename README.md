# 사용자 행동 로그 기반 콘텐츠 분석 시스템

사용자 행동 로그를 기반으로 콘텐츠 성과를 분석하는 데이터 파이프라인 프로젝트입니다.  
네이버 Analytics Engineer 인턴 지원을 위해 아래 역량을 보여주는 데 초점을 맞췄습니다.

- 데이터 기반 문제 해결
- SQL 분석 역량
- 데이터 모델링(fact/dimension) 역량

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

## API (선택)
```bash
uvicorn backend.main:app --reload --port 8000
```
- `GET /health`
- `POST /events`
- `GET /insights/dau`

## 자소서 연결 포인트
- 지원동기/문제해결: 문제 정의 -> 지표 설계 -> 개선 방향 도출 흐름
- SQL 역량: CTE, JOIN, FILTER, WINDOW FUNCTION 기반 분석 쿼리
- 데이터 모델링: fact/dimension + mart + partition 설계
