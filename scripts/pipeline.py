import pandas as pd
import requests
import logging
from io import StringIO
import subprocess

logging.basicConfig(level=logging.INFO)

print("PIPELINE VERSION FINAL RUNNING")

# ------------------------------------
# GOOGLE SHEETS
# ------------------------------------

participants_id = "1phSN8yTzWtnfbvacDIqhqWuD81JKu9DDrzb2q06VdjA"
wages_id = "1x2Uy8L1l0x10YBDLLjIk91shMlTXsMtEPapCssXN1iU"

participants_url = f"https://docs.google.com/spreadsheets/d/{participants_id}/export?format=csv"
wages_url = f"https://docs.google.com/spreadsheets/d/{wages_id}/export?format=csv"


def load_sheet(url):
    r = requests.get(url)
    if r.status_code != 200:
        raise Exception("Could not download sheet")
    return pd.read_csv(StringIO(r.text))


participants = load_sheet(participants_url)
wages = load_sheet(wages_url)

logging.info("Sheets downloaded")

participants.columns = participants.columns.str.strip()
wages.columns = wages.columns.str.strip()

participants.rename(columns={"ID Number": "ID"}, inplace=True)
wages.rename(columns={"ID number/Non SA Passport": "ID"}, inplace=True)

# ------------------------------------
# MERGE DATA
# ------------------------------------

df = pd.merge(participants, wages, on="ID", how="left")

logging.info("Datasets merged")

# ------------------------------------
# CLEAN DATA
# ------------------------------------

df.drop_duplicates(inplace=True)
df.dropna(subset=["ID"], inplace=True)

if "Days worked" in df.columns:
    df["Days worked"] = pd.to_numeric(df["Days worked"], errors="coerce")

if "Nett Wages Paid" in df.columns:
    df["Nett Wages Paid"] = pd.to_numeric(df["Nett Wages Paid"], errors="coerce")

# ------------------------------------
# CALCULATED FIELDS
# ------------------------------------

if "Days worked" in df.columns:
    df["AverageDaysWorked"] = df.groupby("ID")["Days worked"].transform("mean")

if "Nett Wages Paid" in df.columns:
    df["AverageWagesPaid"] = df.groupby("ID")["Nett Wages Paid"].transform("mean")

# ------------------------------------
# SAVE CSV
# ------------------------------------

file_name = "processed_participant_data.csv"

df.to_csv(file_name, index=False)

logging.info("CSV created")
# ------------------------------------
# PUSH FILE TO GITHUB
# ------------------------------------
print("Uploading CSV to GitHub...")

subprocess.run(["git", "config", "--global", "user.name", "github-actions"])
subprocess.run(["git", "config", "--global", "user.email", "actions@github.com"])

subprocess.run(["git", "add", file_name])
subprocess.run(["git", "commit", "-m", "Update processed dataset"], check=False)
subprocess.run(["git", "push"])

print("Upload complete")
