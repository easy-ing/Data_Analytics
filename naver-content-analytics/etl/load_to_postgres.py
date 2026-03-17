import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

RAW_DIR = ROOT / "data" / "raw"

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "analytics")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

users = pd.read_csv(RAW_DIR / "users.csv")
content = pd.read_csv(RAW_DIR / "content.csv")
events = pd.read_csv(RAW_DIR / "events.csv")

users["signup_at"] = pd.to_datetime(users["signup_at"])
content["publish_at"] = pd.to_datetime(content["publish_at"])
events["event_ts"] = pd.to_datetime(events["event_ts"])

users.to_sql("dim_users", engine, schema="analytics", if_exists="append", index=False, method="multi", chunksize=2000)
content.to_sql("dim_content", engine, schema="analytics", if_exists="append", index=False, method="multi", chunksize=2000)
events.to_sql("fact_user_events", engine, schema="analytics", if_exists="append", index=False, method="multi", chunksize=5000)

print(f"Loaded users={len(users)}, content={len(content)}, events={len(events)}")
