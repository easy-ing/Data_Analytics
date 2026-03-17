import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(2026)

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

USERS_PATH = RAW_DIR / "users.csv"
CONTENT_PATH = RAW_DIR / "content.csv"
EVENTS_PATH = RAW_DIR / "events.csv"

NUM_USERS = 500
NUM_CONTENT = 120
NUM_DAYS = 30

age_groups = ["10s", "20s", "30s", "40s", "50+"]
regions = ["Seoul", "Gyeonggi", "Busan", "Daegu", "Incheon"]
channels = ["organic", "search", "social", "push", "ad"]
categories = ["news", "sports", "entertainment", "tech", "lifestyle"]
content_types = ["article", "shortform", "video"]
devices = ["mobile", "desktop", "tablet"]
referrers = ["naver_home", "search", "external", "push"]

start_date = datetime.now() - timedelta(days=NUM_DAYS)

users = []
for i in range(1, NUM_USERS + 1):
    signup_at = start_date - timedelta(days=random.randint(0, 120))
    users.append({
        "user_id": i,
        "signup_at": signup_at.strftime("%Y-%m-%d %H:%M:%S"),
        "age_group": random.choice(age_groups),
        "region": random.choice(regions),
        "acquisition_channel": random.choice(channels),
    })

content = []
for c in range(1, NUM_CONTENT + 1):
    publish_at = start_date - timedelta(days=random.randint(0, 90))
    category = random.choice(categories)
    content.append({
        "content_id": c,
        "content_title": f"{category}_content_{c}",
        "content_category": category,
        "content_type": random.choice(content_types),
        "publish_at": publish_at.strftime("%Y-%m-%d %H:%M:%S"),
    })

rows = []
for d in range(NUM_DAYS):
    day = start_date + timedelta(days=d)
    sessions = random.randint(900, 1400)
    for s in range(sessions):
        u = random.choice(users)
        c = random.choice(content)
        session_id = f"sess_{d}_{s}_{random.randint(1000,9999)}"
        t0 = day + timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )

        rows.append({
            "event_ts": t0.strftime("%Y-%m-%d %H:%M:%S"),
            "user_id": u["user_id"],
            "session_id": session_id,
            "content_id": c["content_id"],
            "event_type": "impression",
            "dwell_seconds": 0,
            "device_type": random.choice(devices),
            "referrer": random.choice(referrers),
            "metadata": "{}",
        })

        if random.random() < 0.35:
            t1 = t0 + timedelta(seconds=random.randint(1, 15))
            rows.append({
                "event_ts": t1.strftime("%Y-%m-%d %H:%M:%S"),
                "user_id": u["user_id"],
                "session_id": session_id,
                "content_id": c["content_id"],
                "event_type": "click",
                "dwell_seconds": 0,
                "device_type": random.choice(devices),
                "referrer": random.choice(referrers),
                "metadata": "{}",
            })

            dwell = random.randint(8, 240)
            t2 = t1 + timedelta(seconds=2)
            t3 = t2 + timedelta(seconds=dwell)
            rows.append({
                "event_ts": t2.strftime("%Y-%m-%d %H:%M:%S"),
                "user_id": u["user_id"],
                "session_id": session_id,
                "content_id": c["content_id"],
                "event_type": "view_start",
                "dwell_seconds": 0,
                "device_type": random.choice(devices),
                "referrer": random.choice(referrers),
                "metadata": "{}",
            })
            rows.append({
                "event_ts": t3.strftime("%Y-%m-%d %H:%M:%S"),
                "user_id": u["user_id"],
                "session_id": session_id,
                "content_id": c["content_id"],
                "event_type": "view_end",
                "dwell_seconds": dwell,
                "device_type": random.choice(devices),
                "referrer": random.choice(referrers),
                "metadata": "{}",
            })

            for ev, p in [("like", 0.2), ("comment", 0.08), ("share", 0.05), ("bookmark", 0.09)]:
                if random.random() < p:
                    rows.append({
                        "event_ts": (t3 + timedelta(seconds=random.randint(1, 4))).strftime("%Y-%m-%d %H:%M:%S"),
                        "user_id": u["user_id"],
                        "session_id": session_id,
                        "content_id": c["content_id"],
                        "event_type": ev,
                        "dwell_seconds": 0,
                        "device_type": random.choice(devices),
                        "referrer": random.choice(referrers),
                        "metadata": "{}",
                    })

for path, data in [(USERS_PATH, users), (CONTENT_PATH, content), (EVENTS_PATH, rows)]:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(data[0].keys()))
        writer.writeheader()
        writer.writerows(data)

print(f"Generated users={len(users)}, content={len(content)}, events={len(rows)}")
print(f"Files: {USERS_PATH}, {CONTENT_PATH}, {EVENTS_PATH}")
