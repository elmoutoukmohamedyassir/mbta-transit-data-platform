from datetime import datetime, timezone

import pytest

from parsing import parse_recorded_at


def test_vehicle_filename_with_folder():
    result = parse_recorded_at("vehicles/vehicles_2026-09-16T18-10-27.json")
    assert result == datetime(2026, 9, 16, 18, 10, 27, tzinfo=timezone.utc)


def test_prediction_filename_with_folder():
    result = parse_recorded_at("predictions/predictions_2026-10-25T21-32-05.json")
    assert result == datetime(2026, 10, 25, 21, 32, 5, tzinfo=timezone.utc)


def test_result_is_timezone_aware_utc():
    result = parse_recorded_at("vehicles/vehicles_2026-09-16T18-10-27.json")
    assert result.tzinfo == timezone.utc


def test_filename_without_folder():
    result = parse_recorded_at("vehicles_2026-01-01T00-00-00.json")
    assert result == datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_end_of_year_boundary():
    result = parse_recorded_at("vehicles/vehicles_2026-12-31T23-59-59.json")
    assert result == datetime(2026, 12, 31, 23, 59, 59, tzinfo=timezone.utc)


def test_bad_timestamp_raises_value_error():
    with pytest.raises(ValueError):
        parse_recorded_at("vehicles/vehicles_not-a-date.json")


def test_missing_underscore_raises_index_error():
    with pytest.raises(IndexError):
        parse_recorded_at("vehicles/garbage.json")