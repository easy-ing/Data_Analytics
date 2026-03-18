# API 명세

Base URL: `http://localhost:8000`

## 1) `GET /health`
서비스 상태 확인.

### Response 200
```json
{
  "status": "ok"
}
```

## 2) `POST /events`
단건 이벤트 수집 API.

### Request Body
```json
{
  "event_ts": "2026-03-18T10:30:00",
  "user_id": 1001,
  "session_id": "sess_abc_001",
  "content_id": 2001,
  "event_type": "click",
  "dwell_seconds": 12,
  "device_type": "mobile",
  "referrer": "naver_home",
  "metadata": {
    "ab_group": "A"
  }
}
```

### Field Spec
- `event_ts` (string, required): ISO-8601 timestamp
- `user_id` (integer, required): 사용자 ID
- `session_id` (string, required): 세션 식별자
- `content_id` (integer, required): 콘텐츠 ID
- `event_type` (string, optional, default=`view`): 이벤트 타입
- `dwell_seconds` (integer, optional, default=`0`): 체류시간(음수 입력 시 0으로 보정)
- `device_type` (string, optional, default=`mobile`)
- `referrer` (string, optional, default=`naver_home`)
- `metadata` (object, optional, default=`{}`)

### Response 200
```json
{
  "inserted": 1
}
```

## 3) `GET /insights/dau`
최근 30일 DAU 조회.

### Response 200
```json
{
  "rows": [
    {
      "event_date": "2026-03-18",
      "dau": 127
    }
  ]
}
```
