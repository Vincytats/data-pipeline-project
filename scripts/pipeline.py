import pandas as pd
import os
import logging

logging.basicConfig(
    filename="pipeline.log",
    level=logging.INFO
)

print("Starting pipeline")

# Load source files
wages = pd.read_excel("Participant_Wages.xlsx")
participants = pd.read_excel("Participant_List.xlsx")

# Standardize ID column
participants.rename(
    columns={"ID number/Non SA Passport": "ID"},
    inplace=True
)

wages.rename(
    columns={"ID Number": "ID"},
    inplace=True
)

# Merge datasets
merged = pd.merge(wages, participants, on="ID", how="left")

# Cleaning
merged.drop_duplicates(inplace=True)
merged.dropna(subset=["ID"], inplace=True)

# Save processed dataset
merged.to_csv("processed_participant_data.csv", index=False)

# Update master dataset
if os.path.exists("master_dataset.csv"):
    master = pd.read_csv("master_dataset.csv")
    combined = pd.concat([master, merged])
    combined.drop_duplicates(inplace=True)
else:
    combined = merged

combined.to_csv("master_dataset.csv", index=False)

print("Pipeline finished")
