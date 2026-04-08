import pandas as pd
import requests
from io import StringIO
import logging
import os

logging.basicConfig(level=logging.INFO)

print("PIPELINE VERSION FINAL RUNNING")

participants_id = "1phSN8yTzWtnfbvacDIqhqWuD81JKu9DDrzb2q06VdjA"
wages_id = "1x2Uy8L1l0x10YBDLLjIk91shMlTXsMtEPapCssXN1iU"

participants_url = f"https://docs.google.com/spreadsheets/d/{participants_id}/export?format=csv"
wages_url = f"https://docs.google.com/spreadsheets/d/{wages_id}/export?format=csv"

OUTPUT_FILE = "Consolidated Participant Data.csv"

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

participants.rename(columns={"ID Number": "ID"}, inplace=True)
wages.rename(columns={"ID number/Non SA Passport": "ID"}, inplace=True)

df = pd.merge(participants, wages, on="ID", how="left")
logging.info("Datasets merged")

df.drop_duplicates(inplace=True)
df.dropna(subset=["ID"], inplace=True)

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

df = df[[c for c in required_columns if c in df.columns]]
df.rename(columns={"ID": "ID Number"}, inplace=True)

df.to_csv(OUTPUT_FILE, index=False)
logging.info(f"CSV created: {OUTPUT_FILE}")

def get_access_token():
    url = f"https://login.microsoftonline.com/{os.environ['AZURE_TENANT_ID']}/oauth2/v2.0/token"

    data = {
        "client_id": os.environ["AZURE_CLIENT_ID"],
        "client_secret": os.environ["AZURE_CLIENT_SECRET"],
        "grant_type": "client_credentials",
        "scope": "https://graph.microsoft.com/.default"
    }

    r = requests.post(url, data=data)
    token_data = r.json()

    if "access_token" not in token_data:
        raise Exception(f"TOKEN ERROR: {token_data}")

    return token_data["access_token"]

def upload_to_sharepoint(file_path):
    logging.info("Uploading to SharePoint...")

    token = get_access_token()

    headers = {
        "Authorization": f"Bearer {token}"
    }

    site_url = "https://graph.microsoft.com/v1.0/sites/thelearningtrust.sharepoint.com:/sites/TheLearningTrust"

    site = requests.get(site_url, headers=headers).json()

    print("SITE:", site)

    if "id" not in site:
        raise Exception(f"Site error: {site}")

    site_id = site["id"]

    drive = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive",
        headers=headers
    ).json()

    print("DRIVE:", drive)

    drive_id = drive["id"]

    file_name = os.path.basename(file_path)

    upload_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/Consolidated data/{file_name}:/content"

    with open(file_path, "rb") as f:
        res = requests.put(upload_url, headers=headers, data=f)

    print("UPLOAD:", res.text)

    if res.status_code not in [200, 201]:
        raise Exception(f"Upload failed: {res.text}")

    logging.info("Upload successful")

upload_to_sharepoint(OUTPUT_FILE)

logging.info("PIPELINE COMPLETE")
