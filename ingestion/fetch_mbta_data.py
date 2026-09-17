import os
import json
import time
from io import BytesIO
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv
from minio import Minio

# ── Setup (runs once) ─────────────────────────────────────────────
load_dotenv()
mbta_api_key = os.getenv("MBTA_API_KEY")
minio_user = os.getenv("MINIO_ROOT_USER")
minio_password = os.getenv("MINIO_ROOT_PASSWORD")

client = Minio(
    "localhost:9000",
    access_key=minio_user,
    secret_key=minio_password,
    secure=False
)

bucket_name = "mbta-raw"
if not client.bucket_exists(bucket_name):
    client.make_bucket(bucket_name)

headers = {"x-api-key": mbta_api_key}


def fetch_and_save(url, params, folder_name):
    response = requests.get(url, params=params, headers=headers)

    if response.status_code == 200:
        data = response.json()
        raw_bytes = json.dumps(data).encode("utf-8")

        now = datetime.now(timezone.utc)
        filename = f"{folder_name}_{now.strftime('%Y-%m-%dT%H-%M-%S')}.json"
        object_path = f"{folder_name}/{filename}"

        client.put_object(
            bucket_name,
            object_path,
            data=BytesIO(raw_bytes),
            length=len(raw_bytes),
            content_type="application/json"
        )
        print(f"Saved: {object_path} ({len(data['data'])} records)")
    else:
        print(f"Error {response.status_code} on {url}: {response.text}")


# ── The loop: repeat forever, every 30 seconds ─────────────────────
while True:
    fetch_and_save(
        url="https://api-v3.mbta.com/vehicles",
        params={"filter[route]": "Red"},
        folder_name="vehicles"
    )
    fetch_and_save(
        url="https://api-v3.mbta.com/predictions",
        params={"filter[route]": "Red"},
        folder_name="predictions"
    )
    time.sleep(30)