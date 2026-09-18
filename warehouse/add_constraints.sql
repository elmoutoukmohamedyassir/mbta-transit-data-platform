ALTER TABLE fact_vehicle_positions
    ADD CONSTRAINT uq_vehicle_position UNIQUE (vehicle_id, recorded_at);

ALTER TABLE fact_predictions
    ADD CONSTRAINT uq_prediction UNIQUE (trip_id, stop_id, recorded_at);