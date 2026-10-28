from datetime import datetime, timezone


def parse_recorded_at(object_name):
    """Extract the snapshot timestamp from a filename like
    'vehicles/vehicles_2026-09-16T18-10-27.json'"""
    filename = object_name.split("/")[-1]              # drop the folder part
    timestamp_part = filename.split("_", 1)[1]           # drop "vehicles_" prefix
    timestamp_part = timestamp_part.replace(".json", "")
    dt = datetime.strptime(timestamp_part, "%Y-%m-%dT%H-%M-%S")
    return dt.replace(tzinfo=timezone.utc)