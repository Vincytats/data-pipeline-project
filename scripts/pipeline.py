import logging
import pandas as pd
import requests
from io import StringIO

# ------------------------------------------------
# LOGGING
# ------------------------------------------------

logging.basicConfig(level=logging.INFO)
logging.info("Pipeline started")

# ------------------------------------------------
# GOOGLE SHEET IDS
# ------------------------------------------------

PARTICIPANT_LIST_ID = "1phSN8yTzWtnfbvacDIqhqWuD81JKu9DDrzb2q06VdjA"
WAGES_ID = "1x2Uy8L1l0x10YBDLLjIk91shMlTXsMtEPapCssXN1iU"

participants_url = f"https://docs.google.com/spreadsheets/d/{PARTICIPANT_LIST_ID}/export?format=csv"
wages_url = f"https://docs.google.com/spreadsheets/d/{WAGES_ID}/export?format=csv"

# ------------------------------------------------
# SAFE DOWNLOAD FUNCTION
# ------------------------------------------------

def load_google_sheet(url):

    for attempt in range(3):
        try:
            response = requests.get(url, timeout=60)

            if response.status_code != 200:
                raise Exception(f"Download failed: {response.status_code}")

            return pd.read_csv(StringIO(response.text))

        except Exception as e:
            logging.warning(f"Attempt {attempt+1} failed: {e}")

    raise Exception("Failed to download Google Sheet")

# ------------------------------------------------
# LOAD DATA
# ------------------------------------------------

participants = load_google_sheet(participants_url)
wages = load_google_sheet(wages_url)

logging.info("Google Sheets loaded successfully")

participants.columns = participants.columns.str.strip()
wages.columns = wages.columns.str.strip()

logging.info(f"Participants columns: {participants.columns.tolist()}")
logging.info(f"Wages columns: {wages.columns.tolist()}")

# ------------------------------------------------
# FIX ID COLUMN NAMES
# ------------------------------------------------

participants.rename(
    columns={"ID Number": "ID"},
    inplace=True
)

wages.rename(
    columns={"ID number/Non SA Passport": "ID"},
    inplace=True
)

# verify ID exists
if "ID" not in participants.columns:
    raise Exception("Participants sheet missing ID column")

if "ID" not in wages.columns:
    raise Exception("Wages sheet missing ID column")

logging.info("ID columns standardized")

# ------------------------------------------------
# MERGE DATASETS
# ------------------------------------------------

df = pd.merge(participants, wages, on="ID", how="left")

logging.info("Datasets merged successfully")

# ------------------------------------------------
# DATA CLEANING
# ------------------------------------------------

df.drop_duplicates(inplace=True)
df.dropna(subset=["ID"], inplace=True)

if "Days worked" in df.columns:
    df["Days worked"] = pd.to_numeric(df["Days worked"], errors="coerce")

if "Nett Wages Paid" in df.columns:
    df["Nett Wages Paid"] = pd.to_numeric(df["Nett Wages Paid"], errors="coerce")

logging.info("Data cleaned")

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
# SAVE DATASET
# ------------------------------------------------

df.to_csv("processed_participant_data.csv", index=False)

logging.info("Dataset saved successfully")

print("Pipeline finished successfully")
