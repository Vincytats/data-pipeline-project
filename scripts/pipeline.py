import pandas as pd
import requests
from io import StringIO
import logging
import calendar
import re
import os

from google.oauth2 import service_account
from googleapiclient.discovery import build

logging.basicConfig(level=logging.INFO)

print("PIPELINE VERSION FINAL RUNNING")

FOLDER_NAME = "Participant Wages Paid Datasource"
OUTPUT_FILE = "Consolidated Participant Data.csv"

SCOPES = ['https://www.googleapis.com/auth/drive']

creds = service_account.Credentials.from_service_account_file(
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
    scopes=SCOPES
)

drive_service = build('drive', 'v3', credentials=creds)

def get_folder_id(folder_name):
    query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder'"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    folders = results.get('files', [])
    if not folders:
        raise Exception("Folder not found")
    return folders[0]['id']

def get_files_in_folder(folder_id):
    query = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet'"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    return results.get('files', [])

def load_sheet(file_id):
    url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=csv"
    r = requests.get(url)
    if r.status_code != 200:
        raise Exception("Failed to download sheet")
    return pd.read_csv(StringIO(r.text), dtype=str)

def clean_columns(df):
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(r"\s+", " ", regex=True)
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

# 🔥 Get folder + files
folder_id = get_folder_id(FOLDER_NAME)
files = get_files_in_folder(folder_id)

logging.info(f"FILES FOUND: {[f['name'] for f in files]}")

all_data = []

for file in files:
    file_id = file["id"]
    file_name = file["name"]

    df = load_sheet(file_id)
    df = clean_columns(df)

    for col in df.columns:
        if "id" in col.lower():
            df.rename(columns={col: "ID"}, inplace=True)
            break

    if "ID" in df.columns:
        df["ID"] = clean_id(df["ID"])

    month_recorded, payment_date = extract_month_info(file_name)

    df["Month_recorded"] = month_recorded
    df["Payment_Date"] = payment_date

    all_data.append(df)

df = pd.concat(all_data, ignore_index=True)

logging.info(f"TOTAL ROWS: {len(df)}")

# remove only missing ID
df = df[df["ID"].notna() & (df["ID"] != "")]

df.rename(columns={"ID": "ID Number"}, inplace=True)

# Excel-safe format
df["ID Number"] = '="' + df["ID Number"] + '"'

df.to_csv(OUTPUT_FILE, index=False)

logging.info(f"CSV created: {OUTPUT_FILE}")
