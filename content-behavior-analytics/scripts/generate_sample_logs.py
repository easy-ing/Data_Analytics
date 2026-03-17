import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

USERS_CSV = RAW_DIR / "users.csv"
CONTENT_CSV = RAW_DIR / "content.csv"
EVENTS_CSV = RAW_DIR / "events.csv"

NUM_USERS = 300
NUM_CONTENT = 80
NUM_DAYS = 30

age_groups = ["10s", "20s", "30s", "40s", "50+"]
genders = ["M", "F", "U"]
regions = ["Seoul", "Gyeonggi", "Busan", "Daegu", "Incheon", "Daejeon"]
channels = ["organic", "search", "social", "push", "ad"]
categories = ["news", "entertainment", "sports", "tech", "lifestyle"]
content_types = ["article", "shortform", "video"]
devices = ["mobile", "desktop", "tablet"]
referrers = ["naver_home", "search", "external", "push"]

start_date = datetime.now() - timedelta(days=NUM_DAYS)


def random_ts(day_offset: int) -> datetime:
    base = start_date + timedelta(days=day_offset)
    return base + timedelta(
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )


# 1) users
users = []
for i in range(1, NUM_USERS + 1):
    signup_at = start_date - timedelta(days=random.randint(0, 120))
    users.append(
        {
            "user_id": i,
            "signup_at": signup_at.strftime("%Y-%m-%d %H:%M:%S"),
            "age_group": random.choice(age_groups),
            "gender": random.choice(genders),
            "region": random.choice(regions),
            "acquisition_channel": random.choice(channels),
        }
    )

# 2) content
content = []
for c in range(1, NUM_CONTENT + 1):
    publish_at = start_date - timedelta(days=random.randint(0, 60))
    category = random.choice(categories)
    ctype = random.choices(content_types, weights=[0.5, 0.3, 0.2], k=1)[0]
    content.append(
        {
            "content_id": c,
            "content_title": f"{category}_content_{c}",
            "content_category": category,
            "content_type": ctype,
            "publish_at": publish_at.strftime("%Y-%m-%d %H:%M:%S"),
            "author_id": random.randint(1000, 1020),
        }
    )

# 3) events
# session-level behavior flow: impression -> click/view_start -> view_end -> optional engagement
event_rows = []
event_types = ["impression", "click", "view_start", "view_end", "like", "comment", "share", "bookmark"]

for day in range(NUM_DAYS):
    daily_sessions = random.randint(700, 1000)
    for s in range(daily_sessions):
        user = random.choice(users)
        c = random.choice(content)
        session_id = f"sess_{day}_{s}_{random.randint(10000,99999)}"
        ts0 = random_ts(day)
        device = random.choice(devices)
        ref = random.choice(referrers)

        # impression (always)
        event_rows.append(
            {
                "event_ts": ts0.strftime("%Y-%m-%d %H:%M:%S"),
                "user_id": user["user_id"],
                "session_id": session_id,
                "content_id": c["content_id"],
                "event_type": "impression",
                "dwell_seconds": 0,
                "device_type": device,
                "referrer": ref,
                "metadata": "{}",
            }
        )

        clicked = random.random() < 0.38
        if clicked:
            ts1 = ts0 + timedelta(seconds=random.randint(1, 20))
            event_rows.append(
                {
                    "event_ts": ts1.strftime("%Y-%m-%d %H:%M:%S"),
                    "user_id": user["user_id"],
                    "session_id": session_id,
                    "content_id": c["content_id"],
                    "event_type": "click",
                    "dwell_seconds": 0,
                    "device_type": device,
                    "referrer": ref,
                    "metadata": "{}",
                }
            )

            ts2 = ts1 + timedelta(seconds=random.randint(1, 10))
            dwell = random.randint(5, 220)
            ts3 = ts2 + timedelta(seconds=dwell)

            event_rows.append(
                {
                    "event_ts": ts2.strftime("%Y-%m-%d %H:%M:%S"),
                    "user_id": user["user_id"],
                    "session_id": session_id,
                    "content_id": c["content_id"],
                    "event_type": "view_start",
                    "dwell_seconds": 0,
                    "device_type": device,
                    "referrer": ref,
                    "metadata": "{}",
                }
            )
            event_rows.append(
                {
                    "event_ts": ts3.strftime("%Y-%m-%d %H:%M:%S"),
                    "user_id": user["user_id"],
                    "session_id": session_id,
                    "content_id": c["content_id"],
                    "event_type": "view_end",
                    "dwell_seconds": dwell,
                    "device_type": device,
                    "referrer": ref,
                    "metadata": "{}",
                }
            )

            # engagement events
            if random.random() < 0.22:
                event_rows.append(
                    {
                        "event_ts": (ts3 + timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S"),
                        "user_id": user["user_id"],
                        "session_id": session_id,
                        "content_id": c["content_id"],
                        "event_type": "like",
                        "dwell_seconds": 0,
                        "device_type": device,
                        "referrer": ref,
                        "metadata": "{}",
                    }
                )
            if random.random() < 0.09:
                event_rows.append(
                    {
                        "event_ts": (ts3 + timedelta(seconds=2)).strftime("%Y-%m-%d %H:%M:%S"),
                        "user_id": user["user_id"],
                        "session_id": session_id,
                        "content_id": c["content_id"],
                        "event_type": "comment",
                        "dwell_seconds": 0,
                        "device_type": device,
                        "referrer": ref,
                        "metadata": "{}",
                    }
                )
            if random.random() < 0.06:
                event_rows.append(
                    {
                        "event_ts": (ts3 + timedelta(seconds=3)).strftime("%Y-%m-%d %H:%M:%S"),
                        "user_id": user["user_id"],
                        "session_id": session_id,
                        "content_id": c["content_id"],
                        "event_type": "share",
                        "dwell_seconds": 0,
                        "device_type": device,
                        "referrer": ref,
                        "metadata": "{}",
                    }
                )
            if random.random() < 0.08:
                event_rows.append(
                    {
                        "event_ts": (ts3 + timedelta(seconds=4)).strftime("%Y-%m-%d %H:%M:%S"),
                        "user_id": user["user_id"],
                        "session_id": session_id,
                        "content_id": c["content_id"],
                        "event_type": "bookmark",
                        "dwell_seconds": 0,
                        "device_type": device,
                        "referrer": ref,
                        "metadata": "{}",
                    }
                )


# Write CSVs
with USERS_CSV.open("w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=list(users[0].keys()))
    writer.writeheader()
    writer.writerows(users)

with CONTENT_CSV.open("w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=list(content[0].keys()))
    writer.writeheader()
    writer.writerows(content)

with EVENTS_CSV.open("w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=list(event_rows[0].keys()))
    writer.writeheader()
    writer.writerows(event_rows)

print(f"Generated: {USERS_CSV}")
print(f"Generated: {CONTENT_CSV}")
print(f"Generated: {EVENTS_CSV}")
print(f"Users={len(users)}, Content={len(content)}, Events={len(event_rows)}")
