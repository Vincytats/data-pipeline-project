import os
import json
import logging
import pandas as pd
import io

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ----------------------------------------------------
# SET PROJECT ROOT DIRECTORY
# ----------------------------------------------------

ROOT_DIR = os.getcwd()

LOG_DIR = os.path.join(ROOT_DIR, "logs")
DATA_DIR = os.path.join(ROOT_DIR, "data")

# ----------------------------------------------------
# CREATE REQUIRED DIRECTORIES
# ----------------------------------------------------

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

# ----------------------------------------------------
# LOGGING
# ----------------------------------------------------

log_file = os.path.join(LOG_DIR, "pipeline.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

print("Pipeline started")
logging.info("Pipeline started")

# ----------------------------------------------------
# GOOGLE DRIVE AUTHENTICATION
# ----------------------------------------------------

creds_json = os.environ["GOOGLE_CREDENTIALS"]
creds_dict = json.loads(creds_json)

credentials = Credentials.from_service_account_info(
    creds_dict,
    scopes=["https://www.googleapis.com/auth/drive"]
)

drive_service = build("drive", "v3", credentials=credentials)

logging.info("Connected to Google Drive")

# ----------------------------------------------------
# GOOGLE DRIVE FILE IDS
# ----------------------------------------------------

PARTICIPANT_LIST_FILE_ID = "1phSN8yTzWtnfbvacDIqhqWuD81JKu9DDrzb2q06VdjA"
WAGES_FILE_ID = "1x2Uy8L1l0x10YBDLLjIk91shMlTXsMtEPapCssXN1iU"

# ----------------------------------------------------
# DOWNLOAD FILE FUNCTION
# ----------------------------------------------------

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


# ----------------------------------------------------
# DOWNLOAD DATA
# ----------------------------------------------------

download_drive_file(PARTICIPANT_LIST_FILE_ID, "Participant_List.xlsx")
download_drive_file(WAGES_FILE_ID, "Participant_Wages.xlsx")

logging.info("Files downloaded")

# ----------------------------------------------------
# LOAD DATA
# ----------------------------------------------------

participants = pd.read_excel("Participant_List.xlsx")
wages = pd.read_excel("Participant_Wages.xlsx")

logging.info("Excel files loaded")

# ----------------------------------------------------
# STANDARDIZE COLUMN NAMES
# ----------------------------------------------------

participants.rename(
    columns={"ID number/Non SA Passport": "ID"},
    inplace=True
)

wages.rename(
    columns={"ID Number": "ID"},
    inplace=True
)

# ----------------------------------------------------
# MERGE DATASETS
# ----------------------------------------------------

df = pd.merge(wages, participants, on="ID", how="left")

logging.info("Datasets merged")

# ----------------------------------------------------
# DATA CLEANING
# ----------------------------------------------------

df.drop_duplicates(inplace=True)

df.dropna(subset=["ID"], inplace=True)

numeric_columns = ["Days worked", "Nett Wages Paid"]

for col in numeric_columns:

    if col in df.columns:

        df[col] = pd.to_numeric(df[col], errors="coerce")

if "Date Paid" in df.columns:

    df["Date Paid"] = pd.to_datetime(df["Date Paid"], errors="coerce")

df.dropna(how="all", inplace=True)

logging.info("Data cleaned")

# ----------------------------------------------------
# CALCULATED FIELDS
# ----------------------------------------------------

if "Days worked" in df.columns:

    df["AverageDaysWorked"] = df.groupby("ID")["Days worked"].transform("mean")

if "Nett Wages Paid" in df.columns:

    df["AverageWagesPaid"] = df.groupby("ID")["Nett Wages Paid"].transform("mean")

if "Age" in df.columns:

    df["YouthAdult"] = df["Age"].apply(
        lambda x: "Youth" if x < 35 else "Adult"
    )

logging.info("Calculated fields created")

# ----------------------------------------------------
# MASTER DATASET HANDLING
# ----------------------------------------------------

MASTER_FILE = os.path.join(DATA_DIR, "master_dataset.csv")
OUTPUT_FILE = os.path.join(DATA_DIR, "processed_participant_data.csv")

if os.path.exists(MASTER_FILE):

    master = pd.read_csv(MASTER_FILE)

    combined = pd.concat([master, df])

    combined.drop_duplicates(inplace=True)

else:

    combined = df

# ----------------------------------------------------
# SAVE DATASETS
# ----------------------------------------------------

combined.to_csv(MASTER_FILE, index=False)
combined.to_csv(OUTPUT_FILE, index=False)

logging.info("Datasets saved")

print("Pipeline finished successfully")
logging.info("Pipeline finished successfully")
