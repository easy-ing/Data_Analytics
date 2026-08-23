[![CI](https://github.com/easy-ing/Data_Analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/easy-ing/Data_Analytics/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)
[![Python](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.python.org/)
[![Release](https://img.shields.io/github/v/release/easy-ing/Data_Analytics?label=release)](https://github.com/easy-ing/Data_Analytics/releases/latest)

# 사용자 행동 로그 기반 콘텐츠 분석 시스템

## 목차
- [간단한 소개](#간단한-소개)
- [주요 내용(요약)](#주요-내용요약)
- [빠른 시작](#빠른-시작)
- [개발 환경 (pre-commit)](#개발-환경-pre-commit)
- [프로젝트 구조](#프로젝트-구조)
- [핵심 파일](#핵심-파일)
- [데이터 모델(요약)](#데이터-모델요약)
- [성공 기준 / 검증](#성공-기준--검증)
- [확장 아이디어](#확장-아이디어)
- [기여 및 연락](#기여-및-연락)

## 간단한 소개
- 사용자 행동 로그(예: view, click, like, share)를 수집·처리·분석해 콘텐츠 성과 지표를 산출하는 엔드투엔드 데이터 파이프라인 프로젝트입니다.
- 이벤트 생성 → 적재(Postgres) → 차원/팩트 기반 DW 설계 → KPI SQL 분석 → 간단한 API 제공까지 포함합니다.

## 주요 내용(요약)
- 데이터 규모(더미): 사용자 2,000명 / 콘텐츠 500개 / 이벤트 100,000건
- 핵심 지표: DAU, CTR, 평균 체류시간, 7일 재방문율, 카테고리별 인기 콘텐츠 등
- 스택: Python, PostgreSQL, Docker, uvicorn

## 빠른 시작
1. 환경
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```
2. PostgreSQL 실행
```bash
docker compose up -d
```
3. DW 스키마 생성
```bash
psql -h localhost -p 5432 -U postgres -d analytics -f sql/03_dw_schema.sql
```
4. 더미 데이터 생성
```bash
python etl/generate_dummy_data.py --seed 20260317 --output-dir data/raw
```
5. CSV → PostgreSQL 적재
```bash
python etl/load_to_postgres.py --raw-dir data/raw
```
6. KPI 실행(예)
```bash
psql -h localhost -p 5432 -U postgres -d analytics -f sql/02_kpi_queries.sql
```

## 개발 환경 (pre-commit)
이 repo는 코드 스타일과 정적분석을 자동화하기 위해 pre-commit 훅과 기본 포맷터/린터를 구성해 두었습니다. 로컬 개발 환경에서 아래를 한 번만 설정하면 이후 커밋시 자동으로 포맷/검사가 실행됩니다.

설치 및 설정
```bash
# 가상환경 활성화 후
pip install -r requirements-dev.txt
# pre-commit 훅 설치
pre-commit install
# (선택) 모든 파일에 대해 한 번 포맷/검사 실행
pre-commit run --all-files
```

주요 훅
- black: 코드 포맷팅
- isort: import 정렬
- ruff: 정적분석(Lint) 및 일부 자동수정
- 기타: trailing-whitespace, end-of-file-fixer, yaml 체크

CI 연동
- GitHub Actions 워크플로(.github/workflows/ci.yml)에서 lint·format·test를 실행합니다. PR 전에 로컬에서 pre-commit을 실행하면 CI 실패 가능성을 줄일 수 있습니다.

문제가 발생하면
- pre-commit이 실행되지 않거나 에러가 발생하면, 아래를 실행해 수동으로 검사하고 수정하세요.
```bash
ruff check . --fix
isort .
black .
```

## 프로젝트 구조
```
naver-content-analytics/
├─ backend/                      # 간단한 API와 서비스 코드
├─ data/
│  ├─ raw/                       # 생성된 샘플 CSV
│  └─ processed/
├─ docs/                         # 데이터 딕셔너리, 플로우 문서
├─ etl/                          # 더미 생성 / 적재 / mart 빌드 스크립트
├─ sql/                          # 스키마 및 KPI 쿼리
├─ .env.example
├─ docker-compose.yml
├─ requirements.txt
└─ README.md
```

## 핵심 파일
- sql/03_dw_schema.sql — DW(차원/팩트) 스키마 정의
- sql/02_kpi_queries.sql — 기본 KPI 쿼리 모음
- sql/04_advanced_analytics.sql — 고급 분석 쿼리
- etl/generate_dummy_data.py — 더미 데이터 생성
- etl/load_to_postgres.py — CSV → Postgres 적재 진입점
- backend/main.py — 간단한 REST API (GET /health, POST /events, GET /insights/dau)

## 데이터 모델(요약)
- analytics.dim_users
- analytics.dim_contents
- analytics.dim_event_types
- analytics.fact_events (월별 range partition 권장)
- analytics.mart_content_daily (일별 요약 마트)

## 성공 기준 / 검증
- 더미 데이터로 KPI 실행 시 합리적 범위의 지표 산출
- ETL 자동화로 CSV → PostgreSQL 적재 정상 동작
- mart 테이블 도입으로 반복 조회 성능 개선 확인
- API로 핵심 지표 조회 가능

## 확장 아이디어
- Airflow/Prefect로 오케스트레이션
- dbt로 모델링·테스트 자동화
- 모니터링/알림(예: 실패 시 Slack) 및 대시보드 연동

## 기여 및 연락
- 이 레포는 학습/연구 목적으로 관리됩니다. 개선사항이나 제안은 PR 또는 Issue로 남겨주세요.
