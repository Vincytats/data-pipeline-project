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
# READ GOOGLE SHEETS DIRECTLY
# ------------------------------------------------

participants_url = f"https://docs.google.com/spreadsheets/d/{PARTICIPANT_LIST_ID}/export?format=csv"
wages_url = f"https://docs.google.com/spreadsheets/d/{WAGES_ID}/export?format=csv"

participants = pd.read_csv(participants_url)
wages = pd.read_csv(wages_url)

logging.info("Google Sheets loaded")

# ------------------------------------------------
# CLEAN DATA
# ------------------------------------------------

participants.rename(
    columns={"ID number/Non SA Passport": "ID"},
    inplace=True
)

wages.rename(
    columns={"ID Number": "ID"},
    inplace=True
)

df = pd.merge(wages, participants, on="ID", how="left")

df.drop_duplicates(inplace=True)
df.dropna(subset=["ID"], inplace=True)

logging.info("Data merged")

# ------------------------------------------------
# CALCULATED FIELDS
# ------------------------------------------------

if "Days worked" in df.columns:
    df["AverageDaysWorked"] = df.groupby("ID")["Days worked"].transform("mean")

if "Nett Wages Paid" in df.columns:
    df["AverageWagesPaid"] = df.groupby("ID")["Nett Wages Paid"].transform("mean")

if "Age" in df.columns:
    df["YouthAdult"] = df["Age"].apply(
        lambda x: "Youth" if x < 35 else "Adult"
    )

logging.info("Calculated fields created")

# ------------------------------------------------
# SAVE DATASET LOCALLY
# ------------------------------------------------

output_file = "processed_participant_data.csv"
df.to_csv(output_file, index=False)

logging.info("Dataset created")

# ------------------------------------------------
# GOOGLE DRIVE FOLDER TO SAVE OUTPUT
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

uploaded_file = drive_service.files().create(
    body=file_metadata,
    media_body=media,
    fields="id"
).execute()

logging.info("File uploaded to Google Drive")

print("Pipeline finished successfully")
