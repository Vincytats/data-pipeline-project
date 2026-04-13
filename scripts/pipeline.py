import io
import os
import json
import pandas as pd
from datetime import datetime
from calendar import monthrange

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

import requests


# ==============================
# ENV VARIABLES
# ==============================
FOLDER_ID = os.getenv("FOLDER_ID")

TENANT_ID = os.getenv("AZURE_TENANT_ID")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")

SHAREPOINT_SITE = "thelearningtrust.sharepoint.com"
SITE_PATH = "/sites/TheLearningTrust"


# ==============================
# GOOGLE DRIVE AUTH
# ==============================
def authenticate_drive():
    creds_dict = json.loads(os.getenv("GOOGLE_CREDENTIALS"))

    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/drive"]
    )

    return build("drive", "v3", credentials=creds)


# ==============================
# GET FILES
# ==============================
def get_files(service):
    results = service.files().list(
        q=f"'{FOLDER_ID}' in parents and (mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or mimeType='application/vnd.google-apps.spreadsheet')",
        fields="files(id, name, mimeType)",
        pageSize=1000
    ).execute()

    return results.get("files", [])


# ==============================
# DOWNLOAD FILE
# ==============================
def download_file(service, file_id, mime_type):

    fh = io.BytesIO()

    if mime_type == "application/vnd.google-apps.spreadsheet":
        request = service.files().export_media(
            fileId=file_id,
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        request = service.files().get_media(fileId=file_id)

    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    fh.seek(0)
    return fh


# ==============================
# PROCESS FILE
# ==============================
def process_file(file_stream, filename):

    df = pd.read_excel(file_stream, dtype=str, sheet_name=0)
    df.columns = df.columns.str.strip()

    required_cols = [
        "ID Number", "Wage category", "Nett Wages Paid",
        "Days worked", "Nett Wages Due", "UIF (Participant)",
        "SDL", "Age", "Gender", "Education", "Youth / Adult",
        "Date Paid"
    ]

    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    df = df[required_cols]

    df["ID Number"] = df["ID Number"].astype(str)

    # Handle Date Paid
    if df["Date Paid"].isnull().all():
        try:
            name_part = filename.replace(".xlsx", "")
            date_obj = datetime.strptime(name_part[:15], "%B %Y")

            last_day = monthrange(date_obj.year, date_obj.month)[1]
            df["Date Paid"] = datetime(date_obj.year, date_obj.month, last_day)
        except:
            df["Date Paid"] = None

    df["Reference"] = filename.replace(".xlsx", "")

    # ✅ VERSION COLUMN
    df["Version"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return df


# ==============================
# GET MICROSOFT ACCESS TOKEN
# ==============================
def get_access_token():

    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials"
    }

    response = requests.post(url, data=data)
    response.raise_for_status()

    return response.json()["access_token"]


# ==============================
# UPLOAD TO SHAREPOINT (FINAL FIX)
# ==============================
def upload_to_sharepoint(file_path):

    access_token = get_access_token()
    filename = os.path.basename(file_path)

    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    # ✅ STEP 1: Get Site ID
    site_url = f"https://graph.microsoft.com/v1.0/sites/{SHAREPOINT_SITE}:{SITE_PATH}"
    site_res = requests.get(site_url, headers=headers)
    site_res.raise_for_status()
    site_id = site_res.json()["id"]

    print("SITE ID:", site_id)

    # ✅ STEP 2: Get Drive ID
    drive_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
    drive_res = requests.get(drive_url, headers=headers)
    drive_res.raise_for_status()

    drives = drive_res.json()["value"]

    drive_id = None
    for d in drives:
        if d["name"] in ["Documents", "Shared Documents"]:
            drive_id = d["id"]
            break

    if not drive_id:
        raise Exception("❌ Could not find Documents library")

    print("DRIVE ID:", drive_id)

    # ✅ STEP 3: Upload file
    upload_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/Consolidated data/{filename}:/content"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    }

    print("UPLOAD URL:", upload_url)

    with open(file_path, "rb") as f:
        response = requests.put(upload_url, headers=headers, data=f)

    if response.status_code in [200, 201]:
        print("✅ Upload successful")
    else:
        print("❌ Upload failed:", response.text)


# ==============================
# MAIN PIPELINE
# ==============================
def run_pipeline():

    print("🚀 Starting pipeline...")

    if not FOLDER_ID:
        raise ValueError("❌ FOLDER_ID missing")

    service = authenticate_drive()
    files = get_files(service)

    if not files:
        print("⚠️ No files found")
        return

    print(f"📂 Found {len(files)} files")

    all_data = []

    for file in files:
        print(f"📄 Processing: {file['name']}")

        try:
            file_stream = download_file(service, file["id"], file["mimeType"])
            df = process_file(file_stream, file["name"])
            all_data.append(df)
        except Exception as e:
            print(f"❌ Error processing {file['name']}: {e}")

    final_df = pd.concat(all_data, ignore_index=True)

    output_file = "consolidated_output.xlsx"
    final_df.to_excel(output_file, index=False)

    print("✅ Consolidation complete")

    upload_to_sharepoint(output_file)

    print("🎉 Pipeline finished successfully")


# ==============================
# ENTRY POINT
# ==============================
if __name__ == "__main__":
    run_pipeline()
