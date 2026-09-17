import os
import requests
import psycopg2
from dotenv import load_dotenv

# ── Setup ────────────────────────────────────────────────────────
load_dotenv()
mbta_api_key = os.getenv("MBTA_API_KEY")
headers = {"x-api-key": mbta_api_key}

conn = psycopg2.connect(
    host="localhost",
    port=5433,
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)
cursor = conn.cursor()


# ── Load routes ──────────────────────────────────────────────────
def load_routes():
    response = requests.get(
        "https://api-v3.mbta.com/routes",
        params={"filter[id]": "Red"},
        headers=headers
    )
    data = response.json()["data"]

    for route in data:
        route_id = route["id"]
        long_name = route["attributes"]["long_name"]
        color = route["attributes"]["color"]

        cursor.execute(
            """
            INSERT INTO dim_routes (route_id, long_name, color)
            VALUES (%s, %s, %s)
            ON CONFLICT (route_id) DO NOTHING
            """,
            (route_id, long_name, color)
        )

    conn.commit()
    print(f"Loaded {len(data)} routes")


# ── Load stops ───────────────────────────────────────────────────
def load_stops():
    response = requests.get(
        "https://api-v3.mbta.com/stops",
        params={"filter[route]": "Red"},
        headers=headers
    )
    data = response.json()["data"]

    for stop in data:
        stop_id = stop["id"]
        stop_name = stop["attributes"]["name"]
        latitude = stop["attributes"]["latitude"]
        longitude = stop["attributes"]["longitude"]

        cursor.execute(
            """
            INSERT INTO dim_stops (stop_id, stop_name, latitude, longitude)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (stop_id) DO NOTHING
            """,
            (stop_id, stop_name, latitude, longitude)
        )

    conn.commit()
    print(f"Loaded {len(data)} stops")


# ── Load trips ───────────────────────────────────────────────────
def load_trips():
    response = requests.get(
        "https://api-v3.mbta.com/trips",
        params={"filter[route]": "Red"},
        headers=headers
    )
    data = response.json()["data"]

    loaded_count = 0
    skipped_count = 0

    for trip in data:
        trip_id = trip["id"]
        headsign = trip["attributes"]["headsign"]
        direction_id = trip["attributes"]["direction_id"]
        route_id = trip["relationships"]["route"]["data"]["id"]

        # Skip trips belonging to routes we haven't loaded (e.g. shuttle
        # buses replacing part of the line during disruptions)
        if route_id != "Red":
            skipped_count += 1
            continue

        cursor.execute(
            """
            INSERT INTO dim_trips (trip_id, route_id, headsign, direction_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (trip_id) DO NOTHING
            """,
            (trip_id, route_id, headsign, direction_id)
        )
        loaded_count += 1

    conn.commit()
    print(f"Loaded {loaded_count} trips (skipped {skipped_count} non-Red-Line trips)")


# ── Run all three, in the correct order ─────────────────────────
load_routes()
load_stops()
load_trips()

cursor.close()
conn.close()