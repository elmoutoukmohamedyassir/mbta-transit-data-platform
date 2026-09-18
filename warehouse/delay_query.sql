WITH actual_arrivals AS (
    SELECT
        trip_id,
        stop_id,
        DATE(recorded_at) AS service_date,
        MIN(recorded_at) AS actual_arrival_time
    FROM fact_vehicle_positions
    WHERE current_status = 'STOPPED_AT'
    GROUP BY trip_id, stop_id, DATE(recorded_at)
),

earliest_predictions AS (
    SELECT DISTINCT ON (trip_id, stop_id, DATE(recorded_at))
        trip_id,
        stop_id,
        DATE(recorded_at) AS service_date,
        arrival_time AS first_predicted_arrival
    FROM fact_predictions
    WHERE arrival_time IS NOT NULL
    ORDER BY trip_id, stop_id, DATE(recorded_at), recorded_at ASC
)

SELECT
    a.trip_id,
    a.stop_id,
    a.service_date,
    p.first_predicted_arrival,
    a.actual_arrival_time,
    EXTRACT(EPOCH FROM (a.actual_arrival_time - p.first_predicted_arrival)) AS delay_seconds
FROM actual_arrivals a
JOIN earliest_predictions p
    ON a.trip_id = p.trip_id
    AND a.stop_id = p.stop_id
    AND a.service_date = p.service_date
ORDER BY delay_seconds DESC;