import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL")

# Google Drive API
GOOGLE_APPLICATION_CREDENTIALS="./.credentials/credentials.json"
PARENT_FOLDER_ID=os.getenv("PARENT_FOLDER_ID")

DATABASE_URL=os.getenv("DATABASE_URL")