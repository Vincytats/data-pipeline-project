import os
import json
import logging
import pandas as pd
import requests
from io import StringIO

import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO)
logging.info("Pipeline started")

# ------------------------------------------------
# GOOGLE AUTH
# ------------------------------------------------

creds_json = os.environ.get("GOOGLE_CREDENTIALS")
creds_dict = json.loads(creds_json)

scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

credentials = Credentials.from_service_account_info(
    creds_dict,
    scopes=scopes
)

gc = gspread.authorize(credentials)

logging.info("Connected to Google Sheets")

# ------------------------------------------------
# SOURCE SHEETS
# ------------------------------------------------

PARTICIPANT_LIST_ID = "1phSN8yTzWtnfbvacDIqhqWuD81JKu9DDrzb2q06VdjA"
WAGES_ID = "1x2Uy8L1l0x10YBDLLjIk91shMlTXsMtEPapCssXN1iU"

participants_url = f"https://docs.google.com/spreadsheets/d/{PARTICIPANT_LIST_ID}/export?format=csv"
wages_url = f"https://docs.google.com/spreadsheets/d/{WAGES_ID}/export?format=csv"

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

# ------------------------------------------------
# FIX ID COLUMNS
# ------------------------------------------------

participants.rename(columns={"ID Number": "ID"}, inplace=True)
wages.rename(columns={"ID number/Non SA Passport": "ID"}, inplace=True)

# ------------------------------------------------
# MERGE DATA
# ------------------------------------------------

df = pd.merge(participants, wages, on="ID", how="left")

logging.info("Datasets merged")

# ------------------------------------------------
# CLEAN DATA
# ------------------------------------------------

df.drop_duplicates(inplace=True)
df.dropna(subset=["ID"], inplace=True)

if "Days worked" in df.columns:
    df["Days worked"] = pd.to_numeric(df["Days worked"], errors="coerce")

if "Nett Wages Paid" in df.columns:
    df["Nett Wages Paid"] = pd.to_numeric(df["Nett Wages Paid"], errors="coerce")

# ------------------------------------------------
# CALCULATED FIELDS
# ------------------------------------------------

if "Days worked" in df.columns:
    df["AverageDaysWorked"] = df.groupby("ID")["Days worked"].transform("mean")

if "Nett Wages Paid" in df.columns:
    df["AverageWagesPaid"] = df.groupby("ID")["Nett Wages Paid"].transform("mean")

if "Age" in df.columns:
    df["YouthAdultGroup"] = df["Age"].apply(lambda x: "Youth" if x < 35 else "Adult")

logging.info("Calculated fields created")

# ------------------------------------------------
# WRITE OUTPUT TO GOOGLE SHEET
# ------------------------------------------------

OUTPUT_SHEET_ID = "PASTE_OUTPUT_SHEET_ID_HERE"

spreadsheet = gc.open_by_key(OUTPUT_SHEET_ID)

try:
    worksheet = spreadsheet.worksheet("data")
except:
    worksheet = spreadsheet.add_worksheet(title="data", rows=1000, cols=50)

worksheet.clear()

data = [df.columns.values.tolist()] + df.values.tolist()

worksheet.update(data)

logging.info("Output written to Google Sheet")

print("Pipeline finished successfully")
