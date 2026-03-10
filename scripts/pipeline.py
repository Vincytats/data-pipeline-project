import logging
import pandas as pd
import requests
from io import StringIO

logging.basicConfig(level=logging.INFO)
logging.info("Pipeline started")

# ----------------------------------------
# GOOGLE SHEET IDS
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

logging.info(f"Participants columns: {participants.columns.tolist()}")
logging.info(f"Wages columns: {wages.columns.tolist()}")

# ----------------------------------------
# RENAME ID COLUMNS
# ----------------------------------------

participants.rename(columns={"ID Number": "ID"}, inplace=True)
wages.rename(columns={"ID number/Non SA Passport": "ID"}, inplace=True)

# ----------------------------------------
# CHECK ID EXISTS
# ----------------------------------------

if "ID" not in participants.columns:
    raise Exception("Participants sheet still missing ID column")

if "ID" not in wages.columns:
    raise Exception("Wages sheet still missing ID column")

logging.info("ID columns standardized")

# ----------------------------------------
# MERGE
# ----------------------------------------

df = pd.merge(participants, wages, on="ID", how="left")

logging.info("Datasets merged successfully")

# ----------------------------------------
# CLEAN DATA
# ----------------------------------------

df.drop_duplicates(inplace=True)
df.dropna(subset=["ID"], inplace=True)

if "Days worked" in df.columns:
    df["Days worked"] = pd.to_numeric(df["Days worked"], errors="coerce")

if "Nett Wages Paid" in df.columns:
    df["Nett Wages Paid"] = pd.to_numeric(df["Nett Wages Paid"], errors="coerce")

logging.info("Data cleaned")

# ----------------------------------------
# CALCULATED FIELDS
# ----------------------------------------

if "Days worked" in df.columns:
    df["AverageDaysWorked"] = df.groupby("ID")["Days worked"].transform("mean")

if "Nett Wages Paid" in df.columns:
    df["AverageWagesPaid"] = df.groupby("ID")["Nett Wages Paid"].transform("mean")

if "Age" in df.columns:
    df["YouthAdultGroup"] = df["Age"].apply(lambda x: "Youth" if x < 35 else "Adult")

logging.info("Calculated fields created")

# ----------------------------------------
# SAVE OUTPUT
# ----------------------------------------

df.to_csv("processed_participant_data.csv", index=False)

logging.info("Dataset saved")

print("Pipeline finished successfully")
