import os
import json
import logging
import pandas as pd
import requests
from io import StringIO

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ----------------------------------------
# LOGGING
# ----------------------------------------

logging.basicConfig(level=logging.INFO)
logging.info("Pipeline started")

# ----------------------------------------
# GOOGLE AUTH
# ----------------------------------------

creds_json = os.environ.get("GOOGLE_CREDENTIALS")

creds_dict = json.loads(creds_json)

credentials = Credentials.from_service_account_info(
    creds_dict,
    scopes=["https://www.googleapis.com/auth/drive"]
)

drive_service = build("drive", "v3", credentials=credentials)

logging.info("Connected to Google Drive")

# ----------------------------------------
# GOOGLE SHEETS
# ----------------------------------------

PARTICIPANT_LIST_ID = "1phSN8yTzWtnfbvacDIqhqWuD81JKu9DDrzb2q06VdjA"
WAGES_ID = "1x2Uy8L1l0x10YBDLLjIk91shMlTXsMtEPapCssXN1iU"

participants_url = f"https://docs.google.com/spreadsheets/d/{PARTICIPANT_LIST_ID}/export?format=csv"
wages_url = f"https://docs.google.com/spreadsheets/d/{WAGES_ID}/export?format=csv"

# ----------------------------------------
# SAFE DOWNLOAD
# ----------------------------------------

def load_google_sheet(url):

    for attempt in range(3):
        try:
            r = requests.get(url, timeout=60)

            if r.status_code != 200:
                raise Exception("Download failed")

            return pd.read_csv(StringIO(r.text))

        except Exception as e:
            logging.warning(f"Attempt {attempt+1} failed: {e}")

    raise Exception("Could not download sheet")


participants = load_google_sheet(participants_url)
wages = load_google_sheet(wages_url)

logging.info("Google Sheets loaded successfully")

participants.columns = participants.columns.str.strip()
wages.columns = wages.columns.str.strip()

# ----------------------------------------
# FIX ID COLUMNS
# ----------------------------------------

participants.rename(columns={"ID Number": "ID"}, inplace=True)
wages.rename(columns={"ID number/Non SA Passport": "ID"}, inplace=True)

# ----------------------------------------
# MERGE
# ----------------------------------------

df = pd.merge(participants, wages, on="ID", how="left")

logging.info("Datasets merged")

# ----------------------------------------
# CLEAN DATA
# ----------------------------------------

df.drop_duplicates(inplace=True)
df.dropna(subset=["ID"], inplace=True)

if "Days worked" in df.columns:
    df["Days worked"] = pd.to_numeric(df["Days worked"], errors="coerce")

if "Nett Wages Paid" in df.columns:
    df["Nett Wages Paid"] = pd.to_numeric(df["Nett Wages Paid"], errors="coerce")

# ----------------------------------------
# CALCULATED FIELDS
# ----------------------------------------

if "Days worked" in df.columns:
    df["AverageDaysWorked"] = df.groupby("ID")["Days worked"].transform("mean")

if "Nett Wages Paid" in df.columns:
    df["AverageWagesPaid"] = df.groupby("ID")["Nett Wages Paid"].transform("mean")

if "Age" in df.columns:
    df["YouthAdultGroup"] = df["Age"].apply(lambda x: "Youth" if x < 35 else "Adult")

# ----------------------------------------
# SAVE DATASET
# ----------------------------------------

output_file = "processed_participant_data.csv"

df.to_csv(output_file, index=False)

logging.info("Dataset saved locally")

# ----------------------------------------
# GOOGLE DRIVE UPLOAD / REPLACE
# ----------------------------------------

FOLDER_ID = "1vzl5Q_sZC3e9uonrNdxkgwneYfwulMu5"
FILE_NAME = "processed_participant_data.csv"

query = f"name='{FILE_NAME}' and '{FOLDER_ID}' in parents and trashed=false"

results = drive_service.files().list(
    q=query,
    fields="files(id, name)"
).execute()

files = results.get("files", [])

media = MediaFileUpload(output_file, mimetype="text/csv")

# ----------------------------------------
# IF FILE EXISTS → UPDATE
# ----------------------------------------

if files:

    file_id = files[0]["id"]

    drive_service.files().update(
        fileId=file_id,
        media_body=media
    ).execute()

    logging.info("Existing file replaced in Google Drive")

# ----------------------------------------
# IF FILE DOESN'T EXIST → CREATE
# ----------------------------------------

else:

    file_metadata = {
        "name": FILE_NAME,
        "parents": [FOLDER_ID]
    }

    drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id"
    ).execute()

    logging.info("File uploaded to Google Drive")

print("Pipeline finished successfully")
