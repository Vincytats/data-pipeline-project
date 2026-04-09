import pandas as pd
import requests
from io import StringIO
import logging
import os
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

def standardize_column(df, keyword, new_name):
    for col in df.columns:
        if keyword in col.lower():
            df.rename(columns={col: new_name}, inplace=True)
            return df
    return df

def clean_id(series):
    return (
        series.astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.replace(r"[^\d]", "", regex=True)
        .str.zfill(13)
    )

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

wages_list = []

for wid, name in wages_ids:
    url = f"https://docs.google.com/spreadsheets/d/{wid}/export?format=csv"
    df = load_sheet(url)

    df = clean_columns(df)
    df = standardize_column(df, "id", "ID")
    df = standardize_column(df, "gender", "Gender")

    df["ID"] = clean_id(df["ID"])

    month_recorded, payment_date = extract_month_info(name)

    df["Month_recorded"] = month_recorded
    df["Payment_Date"] = payment_date

    wages_list.append(df)

df = pd.concat(wages_list, ignore_index=True)

logging.info("All wages files combined")

numeric_columns = [
    "Days worked",
    "Nett Wages Paid",
    "Nett Wages Due",
    "UIF (Participant)",
    "SDL",
    "Age"
]

for col in numeric_columns:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

df = df[
    df["Nett Wages Paid"].notna() &
    df["Days worked"].notna()
]

df = df.sort_values(by=["ID", "Month_recorded", "Nett Wages Paid"], ascending=[True, True, False])
df = df.drop_duplicates(subset=["ID", "Month_recorded"], keep="first")

required_columns = [
    "ID",
    "Wage category",
    "Nett Wages Paid",
    "Days worked",
    "Nett Wages Due",
    "UIF (Participant)",
    "SDL",
    "Age",
    "Gender",
    "Education",
    "Youth / Adult",
    "Date Paid",
    "Date Paid_recoded",
    "New Wage Category",
    "Month_recorded",
    "Payment_Date"
]

df = df[[c for c in required_columns if c in df.columns]]

df.rename(columns={"ID": "ID Number"}, inplace=True)

# 🔥 Force Excel-safe text format
df["ID Number"] = '="' + df["ID Number"] + '"'

df.to_csv(OUTPUT_FILE, index=False)

logging.info(f"CSV created: {OUTPUT_FILE}")
