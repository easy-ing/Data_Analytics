# 사용자 행동 로그 기반 콘텐츠 분석 시스템

사용자 행동 로그를 수집/적재/집계하여 콘텐츠 KPI를 분석하는 PostgreSQL 기반 데이터 파이프라인 프로젝트입니다.

## 프로젝트 범위
- 데이터 생성: 더미 사용자/콘텐츠/이벤트 데이터 생성
- ETL: CSV -> DW 스키마 적재(차원 업서트 + 팩트 적재)
- 모델링: Star Schema + 일자 단위 마트
- 분석: KPI/고급 분석 SQL 제공
- API(선택): 이벤트 수집 및 DAU 조회

## 폴더 구조
```text
naver-content-analytics/
├─ backend/
├─ data/
│  └─ raw/
├─ docs/
│  ├─ api_spec.md
│  ├─ data_dictionary.md
│  ├─ env_spec.md
│  ├─ folder_roles.md
│  ├─ project_flow.md
│  └─ runbook.md
├─ etl/
├─ sql/
│  ├─ 01_schema.sql         # 레거시 단순 스키마(참고)
│  ├─ 02_kpi_queries.sql    # 기본 KPI (DW 기준)
│  ├─ 03_dw_schema.sql      # 메인 DW 스키마
│  └─ 04_advanced_analytics.sql
├─ .env.example
├─ docker-compose.yml
├─ Makefile
├─ requirements.txt
└─ CHANGELOG.md
```

## 실행 순서 (권장)
### 1) DB 실행
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

### 5) ETL 적재
```bash
python etl/load_to_postgres.py --raw-dir data/raw
```

### 6) 마트 집계
```bash
python etl/build_mart.py
```

### 7) KPI 분석
```bash
psql -h localhost -p 5432 -U postgres -d analytics -f sql/02_kpi_queries.sql
psql -h localhost -p 5432 -U postgres -d analytics -f sql/04_advanced_analytics.sql
```

## 핵심 테이블
- `analytics.dim_users`
- `analytics.dim_contents`
- `analytics.dim_event_types`
- `analytics.fact_events` (월 단위 Range Partition)
- `analytics.mart_content_daily`

자세한 스키마는 `docs/data_dictionary.md`를 참고하세요.

## 환경변수
자세한 명세는 `docs/env_spec.md`를 참고하세요.

- `DB_HOST`
- `DB_PORT`
- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`

## API (선택)
```bash
uvicorn backend.main:app --reload --port 8000
```

- `GET /health`
- `POST /events`
- `GET /insights/dau`

요청/응답 예시는 `docs/api_spec.md`를 참고하세요.

## 문서
- 실행/검증 가이드: `docs/runbook.md`
- 프로젝트 흐름: `docs/project_flow.md`
- 데이터 사전: `docs/data_dictionary.md`
- 변경 이력: `CHANGELOG.md`
