from pyspark.sql import SparkSession

# Create (or get an existing) Spark session — this is the entry point
# to everything Spark does, similar to how `conn` was your entry point
# to Postgres.
spark = SparkSession.builder.appName("test").getOrCreate()

# Create a tiny, trivial dataset (numbers 0 through 4) distributed
# across Spark, just to prove the engine itself works end-to-end.
numbers = spark.range(5)

# .collect() pulls the distributed data back into a normal Python list
# so we can see it — this is a "small data" operation you'd only do
# for testing/debugging, never on real large data (defeats the purpose).
result = numbers.collect()
print(result)

# Always cleanly shut down the Spark session when done.
spark.stop()