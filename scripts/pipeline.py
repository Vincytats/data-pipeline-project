import pandas as pd
import requests
from io import StringIO
import logging
import calendar
import re

logging.basicConfig(level=logging.INFO)

print("PIPELINE VERSION FINAL RUNNING")

wages_ids = [
    ("1gcfLed3IjEg8SKoH3R2Q8Coawld5yiLiEvIcI45UOJ8", "Feb 2026 Financial Report"),
    ("11SIHx6STP429fgtf19qV-e8Tk3Rb4xR-t_Bvy8ChpJo", "March 2026 Financial Report")
]

OUTPUT_FILE = "Consolidated Participant Data.csv"

def load_sheet(url):
    r = requests.get(url)
    if r.status_code != 200:
        raise Exception("Failed to download sheet")
    return pd.read_csv(StringIO(r.text), dtype=str)

def clean_columns(df):
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(r"\s+", " ", regex=True)
    return df

def extract_month_info(name):
    match = re.search(r"([A-Za-z]+)\s(\d{4})", name)
    if not match:
        return None, None
    month_str, year = match.groups()

    month_str = month_str.capitalize()

    if month_str in calendar.month_name:
        month_number = list(calendar.month_name).index(month_str)
    else:
        month_number = list(calendar.month_abbr).index(month_str)

    last_day = calendar.monthrange(int(year), month_number)[1]

    return f"{month_str} {year} Report", f"{last_day:02d}/{month_number:02d}/{year}"

def clean_id(series):
    return (
        series.astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.replace(r"[^\d]", "", regex=True)
        .str.zfill(13)
    )

wages_list = []

for wid, name in wages_ids:
    url = f"https://docs.google.com/spreadsheets/d/{wid}/export?format=csv"

    df = load_sheet(url)
    df = clean_columns(df)

    # Try to find ID column automatically
    for col in df.columns:
        if "id" in col.lower():
            df.rename(columns={col: "ID"}, inplace=True)
            break

    if "ID" in df.columns:
        df["ID"] = clean_id(df["ID"])

    month_recorded, payment_date = extract_month_info(name)

    df["Month_recorded"] = month_recorded
    df["Payment_Date"] = payment_date

    wages_list.append(df)

df = pd.concat(wages_list, ignore_index=True)

logging.info(f"TOTAL ROWS PULLED: {len(df)}")

# Rename for output if exists
if "ID" in df.columns:
    df.rename(columns={"ID": "ID Number"}, inplace=True)
    df["ID Number"] = '="' + df["ID Number"] + '"'

df.to_csv(OUTPUT_FILE, index=False)

logging.info(f"CSV created: {OUTPUT_FILE}")
