import argparse
import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path


NUM_USERS = 2000
NUM_CONTENTS = 500
NUM_EVENTS = 100000


CATEGORY_WEIGHTS = {
    "news": 0.24,
    "sports": 0.18,
    "entertainment": 0.22,
    "tech": 0.14,
    "lifestyle": 0.12,
    "finance": 0.10,
}

# Category-level behavior parameters for realistic distribution.
# click_rate: probability(click | view)
# like_rate/share_rate: probability(event | click)
# dwell_mean/dwell_std: seconds (normal distribution, clipped >= 3)
CATEGORY_BEHAVIOR = {
    "news": {"click_rate": 0.17, "like_rate": 0.05, "share_rate": 0.04, "dwell_mean": 45, "dwell_std": 18},
    "sports": {"click_rate": 0.23, "like_rate": 0.08, "share_rate": 0.05, "dwell_mean": 58, "dwell_std": 22},
    "entertainment": {"click_rate": 0.29, "like_rate": 0.14, "share_rate": 0.06, "dwell_mean": 72, "dwell_std": 28},
    "tech": {"click_rate": 0.20, "like_rate": 0.07, "share_rate": 0.03, "dwell_mean": 95, "dwell_std": 35},
    "lifestyle": {"click_rate": 0.15, "like_rate": 0.06, "share_rate": 0.03, "dwell_mean": 85, "dwell_std": 30},
    "finance": {"click_rate": 0.13, "like_rate": 0.04, "share_rate": 0.02, "dwell_mean": 105, "dwell_std": 40},
}


def pick_weighted(rng: random.Random, weighted_dict: dict):
    keys = list(weighted_dict.keys())
    weights = list(weighted_dict.values())
    return rng.choices(keys, weights=weights, k=1)[0]


def make_users(rng: random.Random, now: datetime):
    age_groups = ["10s", "20s", "30s", "40s", "50+"]
    age_weights = [0.08, 0.34, 0.30, 0.19, 0.09]
    genders = ["M", "F", "U"]
    gender_weights = [0.48, 0.48, 0.04]
    regions = ["Seoul", "Gyeonggi", "Busan", "Incheon", "Daegu", "Daejeon", "Gwangju"]
    region_weights = [0.34, 0.24, 0.10, 0.09, 0.08, 0.08, 0.07]
    channels = ["organic", "search", "social", "push", "ad"]
    channel_weights = [0.25, 0.32, 0.13, 0.17, 0.13]

    users = []
    for user_id in range(1, NUM_USERS + 1):
        signup_days_ago = int(abs(rng.gauss(180, 120)))
        signup_at = now - timedelta(days=min(signup_days_ago, 720), hours=rng.randint(0, 23), minutes=rng.randint(0, 59))

        users.append(
            {
                "user_id": user_id,
                "signup_at": signup_at.strftime("%Y-%m-%d %H:%M:%S"),
                "age_group": rng.choices(age_groups, weights=age_weights, k=1)[0],
                "gender": rng.choices(genders, weights=gender_weights, k=1)[0],
                "region": rng.choices(regions, weights=region_weights, k=1)[0],
                "acquisition_channel": rng.choices(channels, weights=channel_weights, k=1)[0],
            }
        )
    return users


def make_contents(rng: random.Random, now: datetime):
    content_types = ["article", "shortform", "video"]
    type_weights = [0.56, 0.22, 0.22]

    contents = []
    for content_id in range(1, NUM_CONTENTS + 1):
        category = pick_weighted(rng, CATEGORY_WEIGHTS)
        publish_days_ago = rng.randint(0, 120)
        publish_at = now - timedelta(days=publish_days_ago, hours=rng.randint(0, 23), minutes=rng.randint(0, 59))
        ctype = rng.choices(content_types, weights=type_weights, k=1)[0]
        title = f"{category}_content_{content_id}"
        author_id = rng.randint(10000, 10250)

        contents.append(
            {
                "content_id": content_id,
                "content_title": title,
                "content_category": category,
                "content_type": ctype,
                "author_id": author_id,
                "publish_at": publish_at.strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    return contents


def random_event_ts(rng: random.Random, start: datetime, end: datetime):
    day_span = (end.date() - start.date()).days
    day_offset = rng.randint(0, max(day_span, 1))
    base = datetime.combine((start + timedelta(days=day_offset)).date(), datetime.min.time())

    # Realistic traffic peak: evening > lunch > morning > deep night
    hours = [8, 9, 10, 11, 12, 13, 18, 19, 20, 21, 22, 23, 0, 1]
    hour_weights = [4, 5, 6, 7, 9, 8, 11, 13, 14, 12, 9, 7, 3, 2]
    hour = rng.choices(hours, weights=hour_weights, k=1)[0]

    return base + timedelta(hours=hour, minutes=rng.randint(0, 59), seconds=rng.randint(0, 59))


def dwell_by_category(rng: random.Random, category: str):
    p = CATEGORY_BEHAVIOR[category]
    dwell = int(rng.gauss(p["dwell_mean"], p["dwell_std"]))
    return max(3, min(dwell, 900))


def stable_event_id(rng: random.Random):
    return f"{rng.getrandbits(128):032x}"


def make_events(rng: random.Random, users: list, contents: list, now: datetime):
    start = now - timedelta(days=30)
    end = now

    device_types = ["mobile", "desktop", "tablet"]
    device_weights = [0.72, 0.22, 0.06]
    referrers = ["home_feed", "search", "push", "external"]
    ref_weights = [0.44, 0.31, 0.17, 0.08]

    # Heavy-user pattern (Pareto-like) for realistic activity concentration.
    user_ids = [u["user_id"] for u in users]
    user_activity = [1 / (i ** 0.48) for i in range(1, len(user_ids) + 1)]
    # Shuffle mapping to avoid deterministic low-id dominance.
    rng.shuffle(user_ids)

    # Content popularity skew
    content_by_id = {c["content_id"]: c for c in contents}
    content_ids = [c["content_id"] for c in contents]
    content_popularity = [1 / (i ** 0.55) for i in range(1, len(content_ids) + 1)]
    rng.shuffle(content_ids)

    events = []
    session_seq = 1

    while len(events) < NUM_EVENTS:
        user_id = rng.choices(user_ids, weights=user_activity, k=1)[0]
        content_id = rng.choices(content_ids, weights=content_popularity, k=1)[0]
        content = content_by_id[content_id]
        category = content["content_category"]
        behavior = CATEGORY_BEHAVIOR[category]

        session_id = f"s_{session_seq}_{rng.randint(100000, 999999)}"
        session_seq += 1

        event_ts = random_event_ts(rng, start, end)
        device = rng.choices(device_types, weights=device_weights, k=1)[0]
        referrer = rng.choices(referrers, weights=ref_weights, k=1)[0]

        # 1) view event (base)
        dwell = dwell_by_category(rng, category)
        events.append(
            {
                "event_id": stable_event_id(rng),
                "event_ts": event_ts.strftime("%Y-%m-%d %H:%M:%S"),
                "user_id": user_id,
                "content_id": content_id,
                "session_id": session_id,
                "event_type": "view",
                "dwell_seconds": dwell,
                "device_type": device,
                "referrer": referrer,
                "metadata": json.dumps({"category": category}, ensure_ascii=True),
            }
        )
        if len(events) >= NUM_EVENTS:
            break

        # 2) click event (category-sensitive CTR + context modifier)
        ctr = behavior["click_rate"]
        if referrer == "search":
            ctr *= 1.15
        if device == "tablet":
            ctr *= 0.92
        ctr = max(0.03, min(ctr, 0.60))

        clicked = rng.random() < ctr
        if clicked:
            events.append(
                {
                    "event_id": stable_event_id(rng),
                    "event_ts": (event_ts + timedelta(seconds=rng.randint(1, 20))).strftime("%Y-%m-%d %H:%M:%S"),
                    "user_id": user_id,
                    "content_id": content_id,
                    "session_id": session_id,
                    "event_type": "click",
                    "dwell_seconds": 0,
                    "device_type": device,
                    "referrer": referrer,
                    "metadata": json.dumps({"category": category}, ensure_ascii=True),
                }
            )
            if len(events) >= NUM_EVENTS:
                break

            # 3) like event
            if rng.random() < behavior["like_rate"]:
                events.append(
                    {
                        "event_id": stable_event_id(rng),
                        "event_ts": (event_ts + timedelta(seconds=rng.randint(10, 50))).strftime("%Y-%m-%d %H:%M:%S"),
                        "user_id": user_id,
                        "content_id": content_id,
                        "session_id": session_id,
                        "event_type": "like",
                        "dwell_seconds": 0,
                        "device_type": device,
                        "referrer": referrer,
                        "metadata": json.dumps({"category": category}, ensure_ascii=True),
                    }
                )
                if len(events) >= NUM_EVENTS:
                    break

            # 4) share event
            if rng.random() < behavior["share_rate"]:
                events.append(
                    {
                        "event_id": stable_event_id(rng),
                        "event_ts": (event_ts + timedelta(seconds=rng.randint(20, 90))).strftime("%Y-%m-%d %H:%M:%S"),
                        "user_id": user_id,
                        "content_id": content_id,
                        "session_id": session_id,
                        "event_type": "share",
                        "dwell_seconds": 0,
                        "device_type": device,
                        "referrer": referrer,
                        "metadata": json.dumps({"category": category}, ensure_ascii=True),
                    }
                )

    return events[:NUM_EVENTS]


def write_csv(path: Path, rows: list):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description="Generate realistic dummy users/contents/events CSVs")
    parser.add_argument("--seed", type=int, default=20260317, help="Random seed for reproducibility")
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(Path(__file__).resolve().parents[1] / "data" / "raw"),
        help="Output directory for CSV files",
    )
    args = parser.parse_args()

    rng = random.Random(args.seed)
    now = datetime.now()
    out_dir = Path(args.output_dir)

    users = make_users(rng, now)
    contents = make_contents(rng, now)
    events = make_events(rng, users, contents, now)

    users_path = out_dir / "users.csv"
    contents_path = out_dir / "contents.csv"
    events_path = out_dir / "events.csv"

    write_csv(users_path, users)
    write_csv(contents_path, contents)
    write_csv(events_path, events)

    print(f"Seed: {args.seed}")
    print(f"Saved users:    {users_path} ({len(users)} rows)")
    print(f"Saved contents: {contents_path} ({len(contents)} rows)")
    print(f"Saved events:   {events_path} ({len(events)} rows)")


if __name__ == "__main__":
    main()
