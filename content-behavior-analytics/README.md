# 사용자 행동 로그 기반 콘텐츠 분석 시스템

네이버 Analytics Engineer 인턴 자소서(지원동기, SQL 역량, 데이터 모델링 경험)를 한 프로젝트로 연결하기 위한 포트폴리오입니다.

## 1) 프로젝트 목표
- 사용자 행동 로그(노출/클릭/체류/좋아요/댓글/공유)를 수집 가능한 구조로 모델링
- PostgreSQL 기반 분석 스키마 구축
- 샘플 로그 생성 -> 적재(ETL) -> KPI 분석 SQL까지 end-to-end 구현

## 2) 폴더 구조
```
content-behavior-analytics/
├─ data/
│  ├─ raw/                  # 생성된 샘플 CSV
│  └─ processed/            # 후처리 산출물(선택)
├─ docs/
├─ scripts/
│  ├─ generate_sample_logs.py
│  └─ etl.py
├─ sql/
│  ├─ 01_schema.sql
│  └─ 02_kpi_queries.sql
├─ requirements.txt
└─ README.md
```

## 3) 데이터 모델 개요
- `analytics.dim_users`: 사용자 속성 차원
- `analytics.dim_content`: 콘텐츠 메타 차원
- `analytics.fact_user_events`: 사용자 행동 이벤트 팩트
- `analytics.mart_content_daily`: 일자/콘텐츠 단위 KPI 마트

모델링 의도:
- 이벤트 로그는 `fact_user_events`에 누적하고,
- 반복 조회 KPI는 `mart_content_daily`로 사전 집계해 성능과 운영성을 확보합니다.

## 4) 실행 방법
### 4-1. 환경 준비
```bash
cd content-behavior-analytics
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 4-2. 샘플 로그 생성
```bash
python scripts/generate_sample_logs.py
```

### 4-3. 스키마 생성
```bash
psql -h localhost -p 5432 -U postgres -d postgres -f sql/01_schema.sql
```

### 4-4. ETL 실행
```bash
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=postgres
export DB_USER=postgres
export DB_PASSWORD=postgres
python scripts/etl.py
```

### 4-5. KPI 분석 SQL 실행
```bash
psql -h localhost -p 5432 -U postgres -d postgres -f sql/02_kpi_queries.sql
```

## 5) 핵심 KPI 예시
- DAU 추이
- 일별 CTR(클릭률)
- 카테고리별 Engagement Rate
- 콘텐츠 타입별 평균 체류시간
- 퍼널 전환율(노출->클릭->시청완료)
- 유입채널별 성과
- 코호트 D+1 리텐션

## 6) 자소서 연결 포인트(요약)
- 지원동기: 콘텐츠/사용자 행동 기반 의사결정을 데이터 파이프라인으로 구현한 경험 강조
- SQL 역량: 윈도우/집계/필터/CTE 기반 KPI 분석 쿼리 설계 및 성능 고려
- 데이터 모델링: 차원/팩트/마트 분리로 확장성과 분석 재사용성 확보

## 7) 확장 아이디어
- Airflow/Prefect로 배치 오케스트레이션
- dbt 모델 테스트(uniqueness, not null) 추가
- 대시보드(Tableau/Looker Studio) 연동
