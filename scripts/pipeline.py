import os
import json
import logging
import pandas as pd

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

import io

# -------------------------------
# CONFIGURATION
# -------------------------------

PARTICIPANT_LIST_FILE_ID = "1phSN8yTzWtnfbvacDIqhqWuD81JKu9DDrzb2q06VdjA"
WAGES_FILE_ID = "1x2Uy8L1l0x10YBDLLjIk91shMlTXsMtEPapCssXN1iU"

MASTER_FILE = "data/master_dataset.csv"
OUTPUT_FILE = "data/processed_participant_data.csv"
LOG_FILE = "logs/pipeline.log"

# -------------------------------
# LOGGING
# -------------------------------

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

print("Pipeline started")

# -------------------------------
# AUTHENTICATE GOOGLE DRIVE
# -------------------------------

creds_json = os.environ["GOOGLE_CREDENTIALS"]
creds_dict = json.loads(creds_json)

credentials = Credentials.from_service_account_info(
    creds_dict,
    scopes=["https://www.googleapis.com/auth/drive"]
)

drive_service = build("drive", "v3", credentials=credentials)

logging.info("Connected to Google Drive")


# -------------------------------
# DOWNLOAD FILE FROM DRIVE
# -------------------------------

def download_drive_file(file_id, filename):

    request = drive_service.files().get_media(fileId=file_id)

    fh = io.BytesIO()

    downloader = MediaIoBaseDownload(fh, request)

    done = False

    while done is False:
        status, done = downloader.next_chunk()

    fh.seek(0)

    with open(filename, "wb") as f:
        f.write(fh.read())

    logging.info(f"Downloaded {filename}")


download_drive_file(PARTICIPANT_LIST_FILE_ID, "Participant_List.xlsx")
download_drive_file(WAGES_FILE_ID, "Participant_Wages.xlsx")

# -------------------------------
# LOAD DATA
# -------------------------------

participants = pd.read_excel("Participant_List.xlsx")
wages = pd.read_excel("Participant_Wages.xlsx")

# -------------------------------
# STANDARDIZE COLUMNS
# -------------------------------

participants.rename(
    columns={"ID number/Non SA Passport": "ID"},
    inplace=True
)

wages.rename(
    columns={"ID Number": "ID"},
    inplace=True
)

# -------------------------------
# MERGE DATASETS
# -------------------------------

df = pd.merge(wages, participants, on="ID", how="left")

logging.info("Datasets merged")

# -------------------------------
# DATA CLEANING
# -------------------------------

df.drop_duplicates(inplace=True)

df.dropna(subset=["ID"], inplace=True)

# Convert numeric columns
numeric_columns = ["Days worked", "Nett Wages Paid"]

for col in numeric_columns:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# Fix date
if "Date Paid" in df.columns:
    df["Date Paid"] = pd.to_datetime(df["Date Paid"], errors="coerce")

# Remove empty rows
df.dropna(how="all", inplace=True)

logging.info("Data cleaning complete")

# -------------------------------
# CALCULATED FIELDS
# -------------------------------

if "Days worked" in df.columns:
    df["AverageDaysWorked"] = df.groupby("ID")["Days worked"].transform("mean")

if "Nett Wages Paid" in df.columns:
    df["AverageWagesPaid"] = df.groupby("ID")["Nett Wages Paid"].transform("mean")

# Youth / Adult grouping
if "Age" in df.columns:
    df["YouthAdult"] = df["Age"].apply(
        lambda x: "Youth" if x < 35 else "Adult"
    )

logging.info("Calculated fields created")

# -------------------------------
# LOAD MASTER DATASET
# -------------------------------

if os.path.exists(MASTER_FILE):

    master = pd.read_csv(MASTER_FILE)

    combined = pd.concat([master, df])

    combined.drop_duplicates(inplace=True)

else:

    combined = df

# -------------------------------
# SAVE MASTER DATASET
# -------------------------------

os.makedirs("data", exist_ok=True)

combined.to_csv(MASTER_FILE, index=False)

logging.info("Master dataset updated")

# -------------------------------
# SAVE PROCESSED DATASET
# -------------------------------

combined.to_csv(OUTPUT_FILE, index=False)

logging.info("Processed dataset saved")

print("Pipeline finished successfully")
