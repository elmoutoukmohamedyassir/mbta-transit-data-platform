import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

load_dotenv()

spark = (
    SparkSession.builder
    .appName("mbta_delay_analysis")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    .config("spark.sql.session.timeZone", "America/New_York")
    .getOrCreate()
)

jdbc_url = f"jdbc:postgresql://localhost:5433/{os.getenv('POSTGRES_DB')}"
connection_properties = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver": "org.postgresql.Driver"
}

# ── Load the two fact tables from Postgres into Spark DataFrames ──
vehicle_positions = spark.read.jdbc(jdbc_url, "fact_vehicle_positions", properties=connection_properties)
predictions = spark.read.jdbc(jdbc_url, "fact_predictions", properties=connection_properties)

# ── Actual arrivals: earliest STOPPED_AT per (trip, stop, day) ──
actual_arrivals = (
    vehicle_positions
    .filter(F.col("current_status") == "STOPPED_AT")
    .withColumn("service_date", F.to_date("recorded_at"))
    .groupBy("trip_id", "stop_id", "service_date")
    .agg(F.min("recorded_at").alias("actual_arrival_time"))
)

# ── Earliest prediction per (trip, stop, day), using a window function ──
window_spec = Window.partitionBy("trip_id", "stop_id", F.to_date("recorded_at")).orderBy("recorded_at")

earliest_predictions = (
    predictions
    .filter(F.col("arrival_time").isNotNull())
    .withColumn("service_date", F.to_date("recorded_at"))
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .select(
        "trip_id", "stop_id", "service_date",
        F.col("arrival_time").alias("first_predicted_arrival")
    )
)

# ── Join and calculate delay in seconds ──
delays = (
    actual_arrivals
    .join(earliest_predictions, ["trip_id", "stop_id", "service_date"])
    .withColumn(
        "delay_seconds",
        F.col("actual_arrival_time").cast("long") - F.col("first_predicted_arrival").cast("long")
    )
)

# ── Aggregate: average delay per stop, per hour of day (Boston time) ──
delay_summary = (
    delays
    .withColumn("hour_of_day", F.hour("actual_arrival_time"))
    .groupBy("stop_id", "hour_of_day")
    .agg(
        F.avg("delay_seconds").alias("avg_delay_seconds"),
        F.count("*").alias("num_observations")
    )
    .orderBy(F.desc("avg_delay_seconds"))
)

delay_summary.show(50, truncate=False)

# ── Write the results back into Postgres as a new table ──
delay_summary.write.jdbc(
    jdbc_url, "fact_delay_summary", mode="overwrite", properties=connection_properties
)

spark.stop()