import os
import json
import logging
import pandas as pd

# ------------------------------
# LOGGING
# ------------------------------

logging.basicConfig(level=logging.INFO)
logging.info("Pipeline started")

# ------------------------------
# GOOGLE SHEET IDS
# ------------------------------

PARTICIPANT_LIST_ID = "1phSN8yTzWtnfbvacDIqhqWuD81JKu9DDrzb2q06VdjA"
WAGES_ID = "1x2Uy8L1l0x10YBDLLjIk91shMlTXsMtEPapCssXN1iU"

# ------------------------------
# LOAD GOOGLE SHEETS
# ------------------------------

participants_url = f"https://docs.google.com/spreadsheets/d/{PARTICIPANT_LIST_ID}/export?format=csv"
wages_url = f"https://docs.google.com/spreadsheets/d/{WAGES_ID}/export?format=csv"

participants = pd.read_csv(participants_url)
wages = pd.read_csv(wages_url)

logging.info("Google Sheets loaded")

# ------------------------------
# PRINT COLUMNS (IMPORTANT)
# ------------------------------

print("Participants Columns:")
print(participants.columns.tolist())

print("Wages Columns:")
print(wages.columns.tolist())

# ------------------------------
# STOP SCRIPT HERE FOR DEBUGGING
# ------------------------------

raise Exception("Stopping pipeline so we can inspect column names")
