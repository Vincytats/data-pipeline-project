import io
import os
import pandas as pd
from datetime import datetime
from calendar import monthrange

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

import requests


# ==============================
# ENV VARIABLES (FROM GITHUB SECRETS)
# ==============================
FOLDER_ID = os.getenv("FOLDER_ID")
ONEDRIVE_UPLOAD_URL = os.getenv("ONEDRIVE_UPLOAD_URL")
ACCESS_TOKEN = os.getenv("ONEDRIVE_ACCESS_TOKEN")


# ==============================
# GOOGLE AUTH
# ==============================
def authenticate_drive():
    creds = service_account.Credentials.from_service_account_info(
        eval(os.getenv("GOOGLE_CREDENTIALS")),
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds)


# ==============================
# GET FILES
# ==============================
def get_files(service):
    results = service.files().list(
        q=f"'{FOLDER_ID}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'",
        fields="files(id, name)"
    ).execute()

    return results.get("files", [])


# ==============================
# DOWNLOAD FILE
# ==============================
def download_file(service, file_id):
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
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

    df = pd.read_excel(file_stream, dtype=str)
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

    # Preserve leading zeros
    df["ID Number"] = df["ID Number"].astype(str)

    # Handle Date Paid
    if df["Date Paid"].isnull().all():
        try:
            date_obj = datetime.strptime(filename[:15], "%B %Y")
            last_day = monthrange(date_obj.year, date_obj.month)[1]
            df["Date Paid"] = datetime(date_obj.year, date_obj.month, last_day)
        except:
            df["Date Paid"] = None

    df["Reference"] = filename.replace(".xlsx", "")

    return df


# ==============================
# UPLOAD TO ONEDRIVE
# ==============================
def upload_to_onedrive(file_path):

    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    }

    with open(file_path, "rb") as f:
        response = requests.put(ONEDRIVE_UPLOAD_URL, headers=headers, data=f)

    if response.status_code in [200, 201]:
        print("Upload successful")
    else:
        print("Upload failed:", response.text)


# ==============================
# MAIN
# ==============================
def run_pipeline():

    service = authenticate_drive()
    files = get_files(service)

    all_data = []

    for file in files:
        print(f"Processing: {file['name']}")

        file_stream = download_file(service, file["id"])
        df = process_file(file_stream, file["name"])

        all_data.append(df)

    final_df = pd.concat(all_data, ignore_index=True)

    output_file = "consolidated_output.xlsx"
    final_df.to_excel(output_file, index=False)

    upload_to_onedrive(output_file)

    print("Pipeline complete")


if __name__ == "__main__":
    run_pipeline()
