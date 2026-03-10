import os
import json
import logging
import pandas as pd
import io

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# ------------------------------------------------
# LOGGING
# ------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Pipeline started")

# ------------------------------------------------
# GOOGLE AUTH
# ------------------------------------------------

creds_json = os.environ.get("GOOGLE_CREDENTIALS")

if not creds_json:
    raise Exception("GOOGLE_CREDENTIALS secret missing")

creds_dict = json.loads(creds_json)

credentials = Credentials.from_service_account_info(
    creds_dict,
    scopes=["https://www.googleapis.com/auth/drive"]
)

drive_service = build("drive", "v3", credentials=credentials)

logging.info("Connected to Google Drive")

# ------------------------------------------------
# GOOGLE SHEET IDS
# ------------------------------------------------

PARTICIPANT_LIST_ID = "1phSN8yTzWtnfbvacDIqhqWuD81JKu9DDrzb2q06VdjA"
WAGES_ID = "1x2Uy8L1l0x10YBDLLjIk91shMlTXsMtEPapCssXN1iU"

# ------------------------------------------------
# LOAD GOOGLE SHEETS
# ------------------------------------------------

participants_url = f"https://docs.google.com/spreadsheets/d/{PARTICIPANT_LIST_ID}/export?format=csv"
wages_url = f"https://docs.google.com/spreadsheets/d/{WAGES_ID}/export?format=csv"

participants = pd.read_csv(participants_url)
wages = pd.read_csv(wages_url)

logging.info("Google Sheets loaded")

# ------------------------------------------------
# CLEAN COLUMN NAMES
# ------------------------------------------------

participants.columns = participants.columns.str.strip().str.lower()
wages.columns = wages.columns.str.strip().str.lower()

logging.info(f"Participants columns: {participants.columns}")
logging.info(f"Wages columns: {wages.columns}")

# ------------------------------------------------
# FIND ID COLUMN AUTOMATICALLY
# ------------------------------------------------

def find_id_column(columns):
    for col in columns:
        if "id" in col:
            return col
    return None

participant_id_col = find_id_column(participants.columns)
wages_id_col = find_id_column(wages.columns)

if participant_id_col is None:
    raise Exception(f"No ID column found in participants sheet: {participants.columns}")

if wages_id_col is None:
    raise Exception(f"No ID column found in wages sheet: {wages.columns}")

participants.rename(columns={participant_id_col: "ID"}, inplace=True)
wages.rename(columns={wages_id_col: "ID"}, inplace=True)

logging.info("ID columns standardized")

# ------------------------------------------------
# MERGE DATASETS
# ------------------------------------------------

df = pd.merge(wages, participants, on="ID", how="left")

logging.info("Datasets merged")

# ------------------------------------------------
# DATA CLEANING
# ------------------------------------------------

df.drop_duplicates(inplace=True)
df.dropna(subset=["ID"], inplace=True)

numeric_cols = ["days worked", "nett wages paid"]

for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

if "date paid" in df.columns:
    df["date paid"] = pd.to_datetime(df["date paid"], errors="coerce")

df.dropna(how="all", inplace=True)

logging.info("Data cleaned")

# ------------------------------------------------
# CALCULATED FIELDS
# ------------------------------------------------

if "days worked" in df.columns:
    df["AverageDaysWorked"] = df.groupby("ID")["days worked"].transform("mean")

if "nett wages paid" in df.columns:
    df["AverageWagesPaid"] = df.groupby("ID")["nett wages paid"].transform("mean")

if "age" in df.columns:
    df["YouthAdult"] = df["age"].apply(lambda x: "Youth" if x < 35 else "Adult")

logging.info("Calculated fields created")

# ------------------------------------------------
# SAVE DATASET
# ------------------------------------------------

output_file = "processed_participant_data.csv"

df.to_csv(output_file, index=False)

logging.info("Dataset saved locally")

# ------------------------------------------------
# UPLOAD TO GOOGLE DRIVE
# ------------------------------------------------

OUTPUT_FOLDER_ID = "1vzl5Q_sZC3e9uonrNdxkgwneYfwulMu5"

file_metadata = {
    "name": output_file,
    "parents": [OUTPUT_FOLDER_ID]
}

media = MediaIoBaseUpload(
    io.FileIO(output_file, "rb"),
    mimetype="text/csv"
)

drive_service.files().create(
    body=file_metadata,
    media_body=media,
    fields="id"
).execute()

logging.info("Dataset uploaded to Google Drive")

print("Pipeline finished successfully")
