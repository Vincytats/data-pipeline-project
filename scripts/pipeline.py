import pandas as pd
import requests
import logging
from io import StringIO
import os

logging.basicConfig(level=logging.INFO)

print("PIPELINE FINAL VERSION RUNNING")

# -----------------------------
# GOOGLE SHEETS
# -----------------------------

participants_id = "1phSN8yTzWtnfbvacDIqhqWuD81JKu9DDrzb2q06VdjA"
wages_id = "1x2Uy8L1l0x10YBDLLjIk91shMlTXsMtEPapCssXN1iU"

participants_url = f"https://docs.google.com/spreadsheets/d/{participants_id}/export?format=csv"
wages_url = f"https://docs.google.com/spreadsheets/d/{wages_id}/export?format=csv"


def load_sheet(url):
    r = requests.get(url)
    return pd.read_csv(StringIO(r.text))


participants = load_sheet(participants_url)
wages = load_sheet(wages_url)

logging.info("Sheets downloaded")

participants.columns = participants.columns.str.strip()
wages.columns = wages.columns.str.strip()

participants.rename(columns={"ID Number": "ID"}, inplace=True)
wages.rename(columns={"ID number/Non SA Passport": "ID"}, inplace=True)

# -----------------------------
# MERGE
# -----------------------------

df = pd.merge(participants, wages, on="ID", how="left")

# -----------------------------
# CLEAN
# -----------------------------

df.drop_duplicates(inplace=True)
df.dropna(subset=["ID"], inplace=True)

if "Days worked" in df.columns:
    df["Days worked"] = pd.to_numeric(df["Days worked"], errors="coerce")

if "Nett Wages Paid" in df.columns:
    df["Nett Wages Paid"] = pd.to_numeric(df["Nett Wages Paid"], errors="coerce")

# -----------------------------
# CALCULATIONS
# -----------------------------

if "Days worked" in df.columns:
    df["AverageDaysWorked"] = df.groupby("ID")["Days worked"].transform("mean")

if "Nett Wages Paid" in df.columns:
    df["AverageWagesPaid"] = df.groupby("ID")["Nett Wages Paid"].transform("mean")

# -----------------------------
# SAVE FILE
# -----------------------------

file_name = "processed_participant_data.csv"
df.to_csv(file_name, index=False)

logging.info("CSV created")

# -----------------------------
# SHAREPOINT UPLOAD
# -----------------------------

sharepoint_url = "https://researchobs814.sharepoint.com/sites/ResearchObs/Shared%20Documents/PROJECT%20-%20TLT%20-%20Documents/Core%20Work/Data/processed_participant_data.csv"

username = os.environ["SP_USERNAME"]
password = os.environ["SP_PASSWORD"]

with open(file_name, "rb") as file:

    r = requests.put(
        sharepoint_url,
        data=file,
        auth=(username, password)
    )

if r.status_code in [200, 201]:
    print("UPLOAD SUCCESSFUL")
else:
    print("UPLOAD FAILED:", r.text)
