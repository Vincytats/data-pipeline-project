import logging
import pandas as pd
import requests
from io import StringIO
import os

from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential

logging.basicConfig(level=logging.INFO)

logging.info("Pipeline started")

PARTICIPANT_LIST_ID = "1phSN8yTzWtnfbvacDIqhqWuD81JKu9DDrzb2q06VdjA"
WAGES_ID = "1x2Uy8L1l0x10YBDLLjIk91shMlTXsMtEPapCssXN1iU"

participants_url = f"https://docs.google.com/spreadsheets/d/{PARTICIPANT_LIST_ID}/export?format=csv"
wages_url = f"https://docs.google.com/spreadsheets/d/{WAGES_ID}/export?format=csv"


def load_sheet(url):

    r = requests.get(url)

    if r.status_code != 200:
        raise Exception("Could not download sheet")

    return pd.read_csv(StringIO(r.text))


participants = load_sheet(participants_url)
wages = load_sheet(wages_url)

logging.info("Google Sheets loaded")

participants.columns = participants.columns.str.strip()
wages.columns = wages.columns.str.strip()

# -----------------------------
# FIX ID
# -----------------------------

participants.rename(columns={"ID Number": "ID"}, inplace=True)
wages.rename(columns={"ID number/Non SA Passport": "ID"}, inplace=True)

# -----------------------------
# MERGE
# -----------------------------

df = pd.merge(participants, wages, on="ID", how="left")

logging.info("Datasets merged")

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

logging.info("Dataset saved")

# -----------------------------
# SHAREPOINT CONNECTION
# -----------------------------

site_url = "https://researchobs814.sharepoint.com/sites/ResearchObs"

client_id = os.environ["AZURE_CLIENT_ID"]
client_secret = os.environ["AZURE_CLIENT_SECRET"]

ctx = ClientContext(site_url).with_credentials(
    ClientCredential(client_id, client_secret)
)

# -----------------------------
# UPLOAD FILE
# -----------------------------

folder_url = "Shared Documents/PROJECT - TLT - Documents/Core Work/Data"

with open(file_name, "rb") as content_file:

    file_content = content_file.read()

ctx.web.get_folder_by_server_relative_url(folder_url).upload_file(
    file_name,
    file_content
).execute_query()

logging.info("File uploaded to SharePoint")

print("Pipeline finished successfully")
