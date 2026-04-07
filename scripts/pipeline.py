import pandas as pd
import requests
from io import StringIO
import logging
import os

# =========================
# LOGGING
# =========================
logging.basicConfig(level=logging.INFO)

print("PIPELINE VERSION FINAL RUNNING")

# =========================
# CONFIG
# =========================
participants_id = "1phSN8yTzWtnfbvacDIqhqWuD81JKu9DDrzb2q06VdjA"
wages_id = "1x2Uy8L1l0x10YBDLLjIk91shMlTXsMtEPapCssXN1iU"

participants_url = f"https://docs.google.com/spreadsheets/d/{participants_id}/export?format=csv"
wages_url = f"https://docs.google.com/spreadsheets/d/{wages_id}/export?format=csv"

OUTPUT_FILE = "processed_participant_data.csv"

# =========================
# LOAD DATA
# =========================
def load_sheet(url):
    r = requests.get(url)
    if r.status_code != 200:
        raise Exception("Failed to download sheet")
    return pd.read_csv(StringIO(r.text))

participants = load_sheet(participants_url)
wages = load_sheet(wages_url)

logging.info("Sheets downloaded")

# =========================
# CLEAN DATA
# =========================
participants.columns = participants.columns.str.strip()
wages.columns = wages.columns.str.strip()

participants.rename(columns={"ID Number": "ID"}, inplace=True)
wages.rename(columns={"ID number/Non SA Passport": "ID"}, inplace=True)

df = pd.merge(participants, wages, on="ID", how="left")

logging.info("Datasets merged")

df.drop_duplicates(inplace=True)
df.dropna(subset=["ID"], inplace=True)

# =========================
# NUMERIC CLEANING
# =========================
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

# =========================
# SELECT COLUMNS
# =========================
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

# =========================
# SAVE FILE
# =========================
df.to_csv(OUTPUT_FILE, index=False)
logging.info(f"CSV created: {OUTPUT_FILE}")

# =========================
# AUTH TOKEN
# =========================
def get_access_token():
    url = f"https://login.microsoftonline.com/{os.environ['AZURE_TENANT_ID']}/oauth2/v2.0/token"

    data = {
        "client_id": os.environ["AZURE_CLIENT_ID"],
        "client_secret": os.environ["AZURE_CLIENT_SECRET"],
        "grant_type": "client_credentials",
        "scope": "https://graph.microsoft.com/.default"
    }

    response = requests.post(url, data=data)
    return response.json()["access_token"]

# =========================
# UPLOAD FUNCTION
# =========================
def upload_to_sharepoint(file_path):
    logging.info("Uploading to SharePoint...")

    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}"}

    # 🔥 CORRECT SITE URL (WITH TRAILING COLON)
    site_url = f"https://graph.microsoft.com/v1.0/sites/{os.environ['SHAREPOINT_SITE_NAME']}:/sites/TheLearningTrust:"

    response = requests.get(site_url, headers=headers)

    print("SITE RESPONSE:", response.json())

    if "id" not in response.json():
        raise Exception(f"Failed to get site ID: {response.text}")

    site_id = response.json()["id"]

    # 🔹 Get drive ID
    drive_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive"
    drive_response = requests.get(drive_url, headers=headers)

    if "id" not in drive_response.json():
        raise Exception(f"Failed to get drive ID: {drive_response.text}")

    drive_id = drive_response.json()["id"]

    file_name = os.path.basename(file_path)

    upload_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{os.environ['SHAREPOINT_FOLDER']}/{file_name}:/content"

    with open(file_path, "rb") as f:
        upload_response = requests.put(upload_url, headers=headers, data=f)

    if upload_response.status_code in [200, 201]:
        logging.info("✅ Upload successful")
    else:
        raise Exception(f"Upload failed: {upload_response.text}")

# =========================
# RUN PIPELINE
# =========================
upload_to_sharepoint(OUTPUT_FILE)

logging.info("🎉 PIPELINE COMPLETE")
