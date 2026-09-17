-- ── Dimension tables (slow-changing reference data) ──────────────

CREATE TABLE dim_routes (
    route_id     VARCHAR(50) PRIMARY KEY,
    long_name    VARCHAR(200),
    color        VARCHAR(10)
);

CREATE TABLE dim_stops (
    stop_id      VARCHAR(50) PRIMARY KEY,
    stop_name    VARCHAR(200),
    latitude     FLOAT,
    longitude    FLOAT
);

CREATE TABLE dim_trips (
    trip_id       VARCHAR(50) PRIMARY KEY,
    route_id      VARCHAR(50) REFERENCES dim_routes(route_id),
    headsign      VARCHAR(200),
    direction_id  SMALLINT
);

-- ── Fact tables (fast-growing event data) ─────────────────────────

CREATE TABLE fact_vehicle_positions (
    id               SERIAL PRIMARY KEY,
    vehicle_id       VARCHAR(50),
    trip_id          VARCHAR(50) REFERENCES dim_trips(trip_id),
    stop_id          VARCHAR(50) REFERENCES dim_stops(stop_id),
    latitude         FLOAT,
    longitude        FLOAT,
    current_status   VARCHAR(30),
    recorded_at      TIMESTAMPTZ
);

CREATE TABLE fact_predictions (
    id               SERIAL PRIMARY KEY,
    trip_id          VARCHAR(50) REFERENCES dim_trips(trip_id),
    stop_id          VARCHAR(50) REFERENCES dim_stops(stop_id),
    vehicle_id       VARCHAR(50),
    arrival_time     TIMESTAMPTZ,
    departure_time   TIMESTAMPTZ,
    recorded_at      TIMESTAMPTZ
);

-- ── Indexes: speed up the queries we'll actually run later ────────

CREATE INDEX idx_vehicle_positions_trip ON fact_vehicle_positions(trip_id);
CREATE INDEX idx_vehicle_positions_time ON fact_vehicle_positions(recorded_at);
CREATE INDEX idx_predictions_trip ON fact_predictions(trip_id);