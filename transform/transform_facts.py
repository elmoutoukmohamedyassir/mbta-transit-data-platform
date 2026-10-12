import os
import json
from datetime import datetime, timezone
import psycopg2
from dotenv import load_dotenv
from minio import Minio

# ── Setup ────────────────────────────────────────────────────────
load_dotenv()

# MINIO_HOST / POSTGRES_HOST default to localhost (running from your terminal)
# and are overridden by Airflow's containers via environment variables.
minio_client = Minio(
    os.getenv("MINIO_HOST", "localhost:9000"),
    access_key=os.getenv("MINIO_ROOT_USER"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    secure=False
)
bucket_name = "mbta-raw"

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=5433,
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)
cursor = conn.cursor()


def parse_recorded_at(object_name):
    """Extract the snapshot timestamp from a filename like
    'vehicles/vehicles_2026-09-16T18-10-27.json'"""
    filename = object_name.split("/")[-1]              # drop the folder part
    timestamp_part = filename.split("_", 1)[1]           # drop "vehicles_" prefix
    timestamp_part = timestamp_part.replace(".json", "")
    dt = datetime.strptime(timestamp_part, "%Y-%m-%dT%H-%M-%S")
    return dt.replace(tzinfo=timezone.utc)


def ensure_trip_exists(trip_id, route_id="Red"):
    """If this trip isn't in dim_trips yet (e.g. a dynamically ADDED
    trip never listed in the static /trips endpoint), create a
    minimal stub row so the foreign key constraint doesn't reject
    our fact row."""
    cursor.execute(
        """
        INSERT INTO dim_trips (trip_id, route_id, headsign, direction_id)
        VALUES (%s, %s, NULL, NULL)
        ON CONFLICT (trip_id) DO NOTHING
        """,
        (trip_id, route_id)
    )


def ensure_stop_exists(stop_id):
    """If this stop isn't in dim_stops yet (e.g. a platform-level
    child stop not included in the station-level /stops response),
    create a minimal stub row so the foreign key constraint doesn't
    reject our fact row."""
    cursor.execute(
        """
        INSERT INTO dim_stops (stop_id, stop_name, latitude, longitude)
        VALUES (%s, NULL, NULL, NULL)
        ON CONFLICT (stop_id) DO NOTHING
        """,
        (stop_id,)
    )


def load_vehicle_positions():
    objects = minio_client.list_objects(bucket_name, prefix="vehicles/", recursive=True)
    inserted = 0

    for obj in objects:
        response = minio_client.get_object(bucket_name, obj.object_name)
        raw = response.read()
        data = json.loads(raw)

        recorded_at = parse_recorded_at(obj.object_name)

        for vehicle in data["data"]:
            attrs = vehicle["attributes"]
            rels = vehicle["relationships"]

            vehicle_id = vehicle["id"]
            trip_id = rels["trip"]["data"]["id"] if rels["trip"]["data"] else None
            stop_id = rels["stop"]["data"]["id"] if rels["stop"]["data"] else None
            latitude = attrs["latitude"]
            longitude = attrs["longitude"]
            current_status = attrs["current_status"]

            if trip_id:
                ensure_trip_exists(trip_id)
            if stop_id:
                ensure_stop_exists(stop_id)

            cursor.execute(
                """
                INSERT INTO fact_vehicle_positions
                    (vehicle_id, trip_id, stop_id, latitude, longitude, current_status, recorded_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (vehicle_id, recorded_at) DO NOTHING
                """,
                (vehicle_id, trip_id, stop_id, latitude, longitude, current_status, recorded_at)
            )
            inserted += 1

        conn.commit()

    print(f"Processed vehicle positions: {inserted} rows attempted")


def load_predictions():
    objects = minio_client.list_objects(bucket_name, prefix="predictions/", recursive=True)
    inserted = 0

    for obj in objects:
        response = minio_client.get_object(bucket_name, obj.object_name)
        raw = response.read()
        data = json.loads(raw)

        recorded_at = parse_recorded_at(obj.object_name)

        for prediction in data["data"]:
            attrs = prediction["attributes"]
            rels = prediction["relationships"]

            trip_id = rels["trip"]["data"]["id"] if rels["trip"]["data"] else None
            stop_id = rels["stop"]["data"]["id"] if rels["stop"]["data"] else None
            vehicle_id = rels["vehicle"]["data"]["id"] if rels["vehicle"]["data"] else None
            arrival_time = attrs["arrival_time"]
            departure_time = attrs["departure_time"]

            if trip_id is None or stop_id is None:
                continue  # can't insert without these, skip safely

            ensure_trip_exists(trip_id)
            ensure_stop_exists(stop_id)

            cursor.execute(
                """
                INSERT INTO fact_predictions
                    (trip_id, stop_id, vehicle_id, arrival_time, departure_time, recorded_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (trip_id, stop_id, recorded_at) DO NOTHING
                """,
                (trip_id, stop_id, vehicle_id, arrival_time, departure_time, recorded_at)
            )
            inserted += 1

        conn.commit()

    print(f"Processed predictions: {inserted} rows attempted")


load_vehicle_positions()
load_predictions()

cursor.close()
conn.close()