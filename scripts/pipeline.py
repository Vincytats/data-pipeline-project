import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Pipeline started")

creds = os.environ.get("GOOGLE_CREDENTIALS")

if not creds:
    raise Exception("GOOGLE_CREDENTIALS secret not found")

logging.info("Credentials detected successfully")

print("Pipeline executed successfully")
