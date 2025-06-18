import pandas as pd
import duckdb
import random
from datetime import datetime
from faker import Faker

# -----------------------------
# 1. Load raw event data
# -----------------------------
df = pd.read_csv('data/event_stream.csv')
df["etl_at_utc"] = datetime.utcnow()
df["event_time"] = pd.to_datetime(df["event_time"])
df["dt"] = df["event_time"].dt.strftime("%Y%m%d")

#  Connect to DuckDB 
con = duckdb.connect("db/hm.duckdb")


con.execute("DROP TABLE IF EXISTS raw_events_d")
con.execute("""
    CREATE TABLE IF NOT EXISTS raw_events_d AS SELECT * FROM df WHERE 0=1
""")
con.register("sample_data", df)
con.execute("INSERT INTO raw_events_d SELECT * FROM sample_data")

print("Step1. Loaded raw_events_d")

# -----------------------------
# 2. Generate dim_users
# -----------------------------
user_ids_df = con.execute("SELECT DISTINCT user_id FROM raw_events_d WHERE user_id IS NOT NULL").fetchdf()
fake = Faker()
sexs = ["F", "M"]
age_groups = [10,20,30,40,50,60,70,80,90,100]
countries = ["SG", "KR", "ID", "MY", "PH", "TH"]

def generate_user_record(user_id):
    return {
        "user_id": user_id,
        "platform": random.choice(sexs),
        "utm_source": random.choice(age_groups),
        "country": random.choice(countries),
        "first_event_time": fake.date_time_between_dates(
            datetime_start=pd.Timestamp("2025-03-01"),
            datetime_end=pd.Timestamp("2025-03-31")
        ),
        "last_event_time": fake.date_time_between_dates(
            datetime_start=pd.Timestamp("2025-06-01"),
            datetime_end=pd.Timestamp("2025-06-14")
        ),
        "etl_at_utc": pd.Timestamp.now()
    }

user_dim_df = pd.DataFrame([generate_user_record(uid) for uid in user_ids_df["user_id"]])
con.execute("DROP TABLE IF EXISTS dim_users")
con.execute("CREATE OR REPLACE TABLE dim_users AS SELECT * FROM user_dim_df")

print("Step2. Created dim_users")

# -----------------------------
# 3. Generate dim_date
# -----------------------------
start_date = "2025-01-01"
end_date = "2025-12-31"
date_range = pd.date_range(start=start_date, end=end_date, freq='D')

dim_date_df = pd.DataFrame({
    "base_date": date_range.strftime("%Y-%m-%d"),
    "yyyymmdd": date_range.strftime("%Y%m%d"),
    "day_name": date_range.day_name(),
    "week_start_date": date_range.to_series().dt.to_period("W").apply(lambda r: r.start_time).dt.strftime("%Y-%m-%d"),
    "week_end_date": date_range.to_series().dt.to_period("W").apply(lambda r: r.end_time).dt.strftime("%Y-%m-%d"),
    "week": date_range.to_series().dt.isocalendar().week.apply(lambda x: f'W{x}'),
    "month_start_date": date_range.to_series().dt.to_period("M").apply(lambda r: r.start_time).dt.strftime("%Y-%m-%d"),
    "month": date_range.to_series().dt.strftime("%Y-%m"),
    "quarter_start_date": date_range.to_series().dt.to_period("Q").apply(lambda r: r.start_time).dt.strftime("%Y-%m-%d"),
    "quarter": date_range.to_series().dt.quarter.apply(lambda x: f'Q{x}'),
    "year_start": date_range.to_series().dt.to_period("Y").apply(lambda r: r.start_time).dt.strftime("%Y-%m-%d"),
    "etl_at_utc": pd.Timestamp.now()
})

con.execute("DROP TABLE IF EXISTS dim_date")
con.execute("CREATE OR REPLACE TABLE dim_date AS SELECT * FROM dim_date_df")

print("Step3. Created dim_date")
