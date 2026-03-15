import pandas as pd
import requests
from io import StringIO
import subprocess
import logging

logging.basicConfig(level=logging.INFO)

print("PIPELINE VERSION FINAL RUNNING")

participants_id = "1phSN8yTzWtnfbvacDIqhqWuD81JKu9DDrzb2q06VdjA"
wages_id = "1x2Uy8L1l0x10YBDLLjIk91shMlTXsMtEPapCssXN1iU"

participants_url = f"https://docs.google.com/spreadsheets/d/{participants_id}/export?format=csv"
wages_url = f"https://docs.google.com/spreadsheets/d/{wages_id}/export?format=csv"


def load_sheet(url):
    r = requests.get(url)
    if r.status_code != 200:
        raise Exception("Failed to download sheet")
    return pd.read_csv(StringIO(r.text))


participants = load_sheet(participants_url)
wages = load_sheet(wages_url)

logging.info("Sheets downloaded")

participants.columns = participants.columns.str.strip()
wages.columns = wages.columns.str.strip()

# Standardise ID column
participants.rename(columns={"ID Number": "ID"}, inplace=True)
wages.rename(columns={"ID number/Non SA Passport": "ID"}, inplace=True)

# Merge datasets
df = pd.merge(participants, wages, on="ID", how="left")

logging.info("Datasets merged")

df.drop_duplicates(inplace=True)
df.dropna(subset=["ID"], inplace=True)

# Convert numeric columns safely
numeric_columns = [
    "Days worked",
    "Nett Wages Paid",
    "Nett Wages Due",
    "UIF (Participant)",
    "SDL",
    "Age"
]

for col in numeric_columns:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# Columns required for dashboards
required_columns = [
    "ID",
    "Wage category",
    "Nett Wages Paid",
    "Days worked",
    "Nett Wages Due",
    "UIF (Participant)",
    "SDL",
    "Age",
    "Gender",
    "Education",
    "Youth / Adult",
    "Date Paid",
    "Date Paid_recoded",
    "New Wage Category"
]

# Keep only columns that exist in the dataset
final_columns = [col for col in required_columns if col in df.columns]

df = df[final_columns]

# Rename ID back for dashboards
df.rename(columns={"ID": "ID Number"}, inplace=True)

file_name = "processed_participant_data.csv"
df.to_csv(file_name, index=False)

logging.info("CSV created")


print("Uploading CSV to GitHub...")

subprocess.run(["git", "config", "--global", "user.name", "github-actions"])
subprocess.run(["git", "config", "--global", "user.email", "actions@github.com"])

subprocess.run(["git", "add", file_name])
subprocess.run(["git", "commit", "-m", "Update processed dataset"], check=False)
subprocess.run(["git", "push", "origin", "main"])

print("Upload complete")
