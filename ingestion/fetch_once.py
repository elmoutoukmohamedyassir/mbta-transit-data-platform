import os
import json
from io import BytesIO
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv
from minio import Minio

load_dotenv()
mbta_api_key = os.getenv("MBTA_API_KEY")

# Note: "host.docker.internal" instead of "localhost" — this script
# is meant to run INSIDE an Airflow container, which cannot reach
# your machine's services via "localhost" the way your own terminal can.
minio_host = os.getenv("MINIO_HOST", "host.docker.internal:9000")

client = Minio(
    minio_host,
    access_key=os.getenv("MINIO_ROOT_USER"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
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
        raise Exception(f"Error {response.status_code} on {url}: {response.text}")


# ── Run ONCE (no loop — Airflow's scheduler provides the repetition) ──
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