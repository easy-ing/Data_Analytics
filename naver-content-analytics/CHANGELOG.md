# Changelog

## 2026-03-18

### Added
- `docs/env_spec.md` 추가 (환경변수 명세)
- `docs/runbook.md` 추가 (실행/검증 절차)
- `docs/api_spec.md` 추가 (API 요청/응답 명세)

### Changed
- `README.md`를 DW 스키마 기준으로 재정리
- `docs/project_flow.md`를 실제 ETL 흐름에 맞게 업데이트
- `sql/02_kpi_queries.sql`를 `fact_events` 기반으로 변경
- `backend/main.py`를 DW 스키마(`dim_*`, `fact_events`) 기준으로 동기화
- `etl/build_mart.py`를 `fact_events` + `dim_event_types` 기준 집계로 변경

### Notes
- `sql/01_schema.sql`은 레거시 단순 스키마로 유지하고, 메인 실행 경로는 `sql/03_dw_schema.sql` 기준입니다.
