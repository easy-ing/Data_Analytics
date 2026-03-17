# Data Dictionary (PostgreSQL)

## 1) analytics.dim_users
- `user_sk` (BIGSERIAL, PK): 분석 조인용 대체키
- `user_id` (BIGINT, UNIQUE): 서비스 사용자 ID(업무키)
- `signup_at` (TIMESTAMP): 가입 시각
- `age_group` (VARCHAR): 연령대
- `gender` (VARCHAR): 성별
- `region` (VARCHAR): 지역
- `acquisition_channel` (VARCHAR): 유입 채널
- `is_active` (BOOLEAN): 활성 사용자 여부
- `created_at`, `updated_at` (TIMESTAMP): 적재/수정 시각

## 2) analytics.dim_contents
- `content_sk` (BIGSERIAL, PK): 분석 조인용 대체키
- `content_id` (BIGINT, UNIQUE): 콘텐츠 ID(업무키)
- `content_title` (TEXT): 콘텐츠 제목
- `content_category` (VARCHAR): 카테고리
- `content_type` (VARCHAR): 콘텐츠 타입(article/video 등)
- `author_id` (BIGINT): 작성자 ID
- `publish_at` (TIMESTAMP): 발행 시각
- `is_published` (BOOLEAN): 공개 상태
- `created_at`, `updated_at` (TIMESTAMP): 적재/수정 시각

## 3) analytics.dim_event_types
- `event_type_sk` (SMALLSERIAL, PK): 이벤트 타입 조인키
- `event_type_code` (VARCHAR, UNIQUE): 이벤트 코드
- `event_group` (VARCHAR): 상위 그룹(exposure/consumption/engagement)
- `description` (TEXT): 이벤트 설명
- `created_at` (TIMESTAMP): 생성 시각

## 4) analytics.fact_events
- `event_sk` (BIGSERIAL): 파티션 내 팩트 식별 키
- `event_id` (UUID, UNIQUE): 중복 적재 방지용 이벤트 고유 ID
- `event_ts` (TIMESTAMP): 이벤트 발생 시각
- `event_date` (DATE): 파티션/집계 기준 날짜
- `user_id`, `content_id` (BIGINT): 업무 추적용 원본 키
- `user_sk`, `content_sk`, `event_type_sk` (FK): 차원 조인 키
- `session_id` (VARCHAR): 세션 식별자
- `dwell_seconds` (INT): 체류시간(초)
- `scroll_depth` (NUMERIC): 스크롤 비율(선택)
- `device_type` (VARCHAR): 디바이스 타입
- `referrer` (VARCHAR): 유입 경로
- `metadata` (JSONB): 확장 속성
- `created_at` (TIMESTAMP): 적재 시각

## 5) analytics.mart_content_daily
- `stat_date` (DATE, PK): 집계 일자
- `content_sk` (BIGINT, PK/FK): 콘텐츠 차원 키
- `impressions`, `clicks`, `unique_users`, `view_completions` (INT): 소비/전환 지표
- `likes`, `comments`, `shares`, `bookmarks` (INT): 반응 지표
- `avg_dwell_seconds` (NUMERIC): 평균 체류시간
- `ctr` (NUMERIC): 클릭률
- `engagement_rate` (NUMERIC): 참여율
- `created_at`, `updated_at` (TIMESTAMP): 생성/수정 시각
