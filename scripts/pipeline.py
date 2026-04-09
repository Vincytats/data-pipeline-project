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

participants_id = "1x2Uy8L1l0x10YBDLLjIk91shMlTXsMtEPapCssXN1iU"

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

def standardize_id_column(df):
    for col in df.columns:
        col_clean = col.lower().strip()
        if "id number" in col_clean or "id number/non sa passport" in col_clean or col_clean == "id":
            df.rename(columns={col: "ID"}, inplace=True)
            return df
    raise Exception(f"ID column not found in columns: {df.columns.tolist()}")

def clean_id(series):
    return (
        series.astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.replace(r"[^\d]", "", regex=True)
        .str.strip()
    )

def extract_month_info(name):
    match = re.search(r"([A-Za-z]+)\s(\d{4})", name)
    if not match:
        return None, None
    month_str, year = match.groups()

    month_str = month_str.capitalize()

    if month_str in calendar.month_name:
        month_number = list(calendar.month_name).index(month_str)
    elif month_str in calendar.month_abbr:
        month_number = list(calendar.month_abbr).index(month_str)
    else:
        raise ValueError(f"Invalid month: {month_str}")

    last_day = calendar.monthrange(int(year), month_number)[1]

    return f"{month_str} {year} Report", f"{last_day:02d}/{month_number:02d}/{year}"

participants_url = f"https://docs.google.com/spreadsheets/d/{participants_id}/export?format=csv"
participants = load_sheet(participants_url)

participants = clean_columns(participants)
participants = standardize_id_column(participants)
participants["ID"] = clean_id(participants["ID"])

wages_list = []

for wid, name in wages_ids:
    wages_url = f"https://docs.google.com/spreadsheets/d/{wid}/export?format=csv"
    wages = load_sheet(wages_url)

    wages = clean_columns(wages)
    wages = standardize_id_column(wages)
    wages["ID"] = clean_id(wages["ID"])

    month_recorded, payment_date = extract_month_info(name)

    wages["Month_recorded"] = month_recorded
    wages["Payment_Date"] = payment_date

    wages_list.append(wages)

wages = pd.concat(wages_list, ignore_index=True)

logging.info("Sheets downloaded and combined")

df = pd.merge(participants, wages, on="ID", how="left")
logging.info("Datasets merged")

df.dropna(subset=["ID"], inplace=True)

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

df = df.dropna(subset=["Gender"])

df.rename(columns={"ID": "ID Number"}, inplace=True)

df.to_csv(OUTPUT_FILE, index=False)

logging.info(f"CSV created: {OUTPUT_FILE}")

def get_access_token():
    url = f"https://login.microsoftonline.com/{os.environ['AZURE_TENANT_ID']}/oauth2/v2.0/token"

    data = {
        "client_id": os.environ["AZURE_CLIENT_ID"],
        "client_secret": os.environ["AZURE_CLIENT_SECRET"],
        "grant_type": "client_credentials",
        "scope": "https://graph.microsoft.com/.default"
    }

    r = requests.post(url, data=data)
    token_data = r.json()

    if "access_token" not in token_data:
        raise Exception(f"TOKEN ERROR: {token_data}")

    return token_data["access_token"]

def upload_to_sharepoint(file_path):
    logging.info("Uploading to SharePoint...")

    token = get_access_token()

    headers = {
        "Authorization": f"Bearer {token}"
    }

    site_url = "https://graph.microsoft.com/v1.0/sites/thelearningtrust.sharepoint.com:/sites/TheLearningTrust"

    site = requests.get(site_url, headers=headers).json()

    if "id" not in site:
        raise Exception(f"Site error: {site}")

    site_id = site["id"]

    drive = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive",
        headers=headers
    ).json()

    drive_id = drive["id"]

    file_name = os.path.basename(file_path)

    upload_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/Consolidated data/{file_name}:/content"

    with open(file_path, "rb") as f:
        res = requests.put(upload_url, headers=headers, data=f)

    if res.status_code not in [200, 201]:
        raise Exception(f"Upload failed: {res.text}")

    logging.info("Upload successful")

upload_to_sharepoint(OUTPUT_FILE)

logging.info("PIPELINE COMPLETE")
