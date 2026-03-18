# 환경변수 명세

아래 환경변수는 `backend/*`, `etl/*`에서 공통으로 사용합니다.

| 변수명 | 기본값 | 필수 | 설명 | 예시 |
|---|---|---|---|---|
| `DB_HOST` | `localhost` | Y | PostgreSQL 호스트 | `localhost` |
| `DB_PORT` | `5432` | Y | PostgreSQL 포트 | `5432` |
| `DB_NAME` | `analytics` | Y | 접속 DB 이름 | `analytics` |
| `DB_USER` | `postgres` | Y | DB 사용자 | `postgres` |
| `DB_PASSWORD` | `postgres` | Y | DB 비밀번호 | `postgres` |

## 설정 방법
1. `.env.example`를 복사해 `.env`를 생성합니다.
2. 실행 환경에 맞게 값을 수정합니다.

```bash
cp .env.example .env
```

## 주의사항
- 로컬 개발용 기본값(`postgres/postgres`)은 운영 환경에서 사용하지 않습니다.
- CI/CD에서는 `.env` 파일 대신 시크릿 주입 방식을 권장합니다.
