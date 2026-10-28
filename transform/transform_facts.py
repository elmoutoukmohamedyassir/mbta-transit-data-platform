import os
import json
import psycopg2
from dotenv import load_dotenv
from minio import Minio

from parsing import parse_recorded_at

# ── Setup ────────────────────────────────────────────────────────
load_dotenv()

# Storage backend: local MinIO by default, AWS S3 when env vars say so.
# STORAGE_ENDPOINT / STORAGE_SECURE / STORAGE_REGION / STORAGE_ACCESS_KEY /
# STORAGE_SECRET_KEY / BUCKET_NAME override the MinIO defaults below.
# MINIO_HOST / POSTGRES_HOST default to localhost (running from your terminal)
# and are overridden by Airflow's containers via environment variables.
storage_endpoint = (
    os.getenv("STORAGE_ENDPOINT")
    or os.getenv("MINIO_HOST", "localhost:9000")
)
storage_secure = os.getenv("STORAGE_SECURE", "false").lower() == "true"

minio_client = Minio(
    storage_endpoint,
    access_key=os.getenv("STORAGE_ACCESS_KEY") or os.getenv("MINIO_ROOT_USER"),
    secret_key=os.getenv("STORAGE_SECRET_KEY") or os.getenv("MINIO_ROOT_PASSWORD"),
    secure=storage_secure,
    region=os.getenv("STORAGE_REGION") or None,
)
bucket_name = os.getenv("BUCKET_NAME", "mbta-raw")

# A file that failed this many times is skipped until you delete its row
# from failed_files (so a permanently corrupt file stops being retried).
MAX_ATTEMPTS = 5

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=5433,
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)
cursor = conn.cursor()

# ── Bookkeeping tables ───────────────────────────────────────────
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS processed_files (
        object_name  TEXT PRIMARY KEY,
        processed_at TIMESTAMPTZ DEFAULT NOW()
    )
    """
)
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS failed_files (
        object_name    TEXT PRIMARY KEY,
        error_message  TEXT,
        attempts       INT NOT NULL DEFAULT 1,
        last_failed_at TIMESTAMPTZ DEFAULT NOW()
    )
    """
)
conn.commit()

cursor.execute("SELECT object_name FROM processed_files")
processed = {row[0] for row in cursor.fetchall()}

cursor.execute(
    "SELECT object_name FROM failed_files WHERE attempts >= %s", (MAX_ATTEMPTS,)
)
given_up = {row[0] for row in cursor.fetchall()}

# ── Dimension IDs we already know exist (avoids repeating the same insert) ──
known_trips = set()
known_stops = set()


def refresh_known_dimensions():
    """Reload the known-ID sets from the database. Needed after a rollback,
    because rolled-back stub rows no longer exist in dim_trips / dim_stops."""
    cursor.execute("SELECT trip_id FROM dim_trips")
    known_trips.clear()
    known_trips.update(row[0] for row in cursor.fetchall())
    cursor.execute("SELECT stop_id FROM dim_stops")
    known_stops.clear()
    known_stops.update(row[0] for row in cursor.fetchall())


refresh_known_dimensions()


def ensure_trip_exists(trip_id, route_id="Red"):
    """If this trip isn't in dim_trips yet (e.g. a dynamically ADDED
    trip never listed in the static /trips endpoint), create a
    minimal stub row so the foreign key constraint doesn't reject
    our fact row."""
    if trip_id in known_trips:
        return
    cursor.execute(
        """
        INSERT INTO dim_trips (trip_id, route_id, headsign, direction_id)
        VALUES (%s, %s, NULL, NULL)
        ON CONFLICT (trip_id) DO NOTHING
        """,
        (trip_id, route_id)
    )
    known_trips.add(trip_id)


def ensure_stop_exists(stop_id):
    """If this stop isn't in dim_stops yet (e.g. a platform-level
    child stop not included in the station-level /stops response),
    create a minimal stub row so the foreign key constraint doesn't
    reject our fact row."""
    if stop_id in known_stops:
        return
    cursor.execute(
        """
        INSERT INTO dim_stops (stop_id, stop_name, latitude, longitude)
        VALUES (%s, NULL, NULL, NULL)
        ON CONFLICT (stop_id) DO NOTHING
        """,
        (stop_id,)
    )
    known_stops.add(stop_id)


def read_snapshot(object_name):
    response = minio_client.get_object(bucket_name, object_name)
    try:
        return json.loads(response.read())
    finally:
        response.close()
        response.release_conn()


def mark_processed(object_name):
    """Record the file as done, and clear any earlier failure record.
    Not committed here: the caller commits rows + this together."""
    cursor.execute(
        "INSERT INTO processed_files (object_name) VALUES (%s) ON CONFLICT DO NOTHING",
        (object_name,)
    )
    cursor.execute(
        "DELETE FROM failed_files WHERE object_name = %s", (object_name,)
    )


def record_failure(object_name, error):
    """Log (or update) a failure for this file, then commit it."""
    cursor.execute(
        """
        INSERT INTO failed_files (object_name, error_message)
        VALUES (%s, %s)
        ON CONFLICT (object_name) DO UPDATE
        SET attempts       = failed_files.attempts + 1,
            error_message  = EXCLUDED.error_message,
            last_failed_at = NOW()
        """,
        (object_name, str(error)[:500])
    )
    conn.commit()


def insert_vehicle_rows(data, recorded_at):
    """Insert every vehicle in one snapshot. Returns how many rows were attempted."""
    rows = 0
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
        rows += 1
    return rows


def insert_prediction_rows(data, recorded_at):
    """Insert every prediction in one snapshot. Returns how many rows were attempted."""
    rows = 0
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
        rows += 1
    return rows


def process_new_files(prefix, label, insert_rows):
    """Load every unprocessed file under `prefix`. One bad file is logged
    in failed_files and skipped; it never blocks the files after it."""
    files_done = 0
    files_failed = 0
    rows = 0

    objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
    for obj in objects:
        name = obj.object_name
        if name in processed or name in given_up:
            continue  # already loaded, or failed too many times

        try:
            data = read_snapshot(name)
            recorded_at = parse_recorded_at(name)
            file_rows = insert_rows(data, recorded_at)

            # rows + "this file is done" are saved together, or not at all
            mark_processed(name)
            conn.commit()

            processed.add(name)
            rows += file_rows
            files_done += 1
        except Exception as error:
            conn.rollback()               # undo this file's partial inserts
            refresh_known_dimensions()    # forget stub rows that were rolled back
            record_failure(name, error)
            files_failed += 1
            print(f"FAILED {name}: {error}", flush=True)

    print(
        f"{label}: {files_done} new files, {rows} rows attempted, {files_failed} failed",
        flush=True
    )


process_new_files("vehicles/", "Vehicle positions", insert_vehicle_rows)
process_new_files("predictions/", "Predictions", insert_prediction_rows)

cursor.close()
conn.close()