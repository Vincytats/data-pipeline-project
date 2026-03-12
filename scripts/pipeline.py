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

from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential

site_url = "https://researchobs814.sharepoint.com/sites/ResearchObs"

username = os.environ["SP_USERNAME"]
password = os.environ["SP_PASSWORD"]

ctx = ClientContext(site_url).with_credentials(
    UserCredential(username, password)
)

folder_url = "Shared Documents/PROJECT - TLT - Documents/Core Work/Data"

with open(file_name, "rb") as content_file:
    ctx.web.get_folder_by_server_relative_url(folder_url) \
        .upload_file(file_name, content_file.read()) \
        .execute_query()

print("UPLOAD COMPLETE")
