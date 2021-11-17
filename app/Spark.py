from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, expr, count
from pyspark.sql.functions import window
from pyspark.sql.types import StringType, IntegerType, StructType, StructField

from app.Resources import Resources

"""
PySpark implementation of read kafka stream, aggregate data and write it back to kafka on other stream.
"""
# Start Spark Session
spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()

# Read kafka stream
df = (spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", Resources.KAFKA_URI)  # kafka server
      .option("subscribe", Resources.TOPIC_RAW_DATA)  # topic
      # .option("startingOffsets", "earliest")  # start from beginning
      .load())

# Map JSON Event structure
user_schema = StructType([
    StructField("imię", StringType(), True),
    StructField("nazwisko", StringType(), True),
    StructField("wiek", IntegerType(), True),
    StructField("miasto zamieszkania", StringType(), True),
])

# Select json event to columns.
df = (
    df.selectExpr("CAST(value as string)", "timestamp")
        .select(from_json(col("value"), user_schema).alias("json_value"), "timestamp")
        .selectExpr("json_value.*", "timestamp")
        .select(
        col("imię"),
        col("nazwisko"),
        col("wiek"),
        col("miasto zamieszkania"),
        col("timestamp")
    )

)

# Aggregate data with usage of time windows and watermark.
windowedAvg = (
    df.withWatermark("timestamp", "2 minutes")
        .groupBy(window(col("timestamp"), "1 minute").alias('eventTimeWindow'), col('miasto zamieszkania'))
        .agg(avg("wiek").alias("avg_wiek"), count("*").alias("count"))
        .select(
        col("eventTimeWindow.start").alias("eventTime"),
        col("avg_wiek"),
        col('count'),
        col('miasto zamieszkania')
    )
)

# Write to console for debug, this is not necessary.
qk = windowedAvg.selectExpr("CAST(eventTime AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .outputMode('update') \
    .format('console') \
    .option('truncate', 'true') \
    .start()

# Write back to kafka aggregated data.
query = windowedAvg.selectExpr("CAST(eventTime AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format('kafka') \
    .option("kafka.bootstrap.servers", Resources.KAFKA_URI) \
    .option("topic", Resources.TOPIC_COMPUTED_DATA) \
    .option("checkpointLocation", "/kafkaStream") \
    .outputMode("update") \
    .start()

# Make it run.
query.awaitTermination()
