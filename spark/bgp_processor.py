import os
import sys
import logging

logging.basicConfig(level=logging.INFO, stream=sys.stdout, force=True)
logger = logging.getLogger(__name__)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, expr, current_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, ArrayType
)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
POSTGRES_HOST   = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_DB     = os.environ.get("POSTGRES_DB", "bgp_dns_db")
POSTGRES_USER   = os.environ.get("POSTGRES_USER", "admin")
POSTGRES_PASS   = os.environ.get("POSTGRES_PASSWORD", "admin123")
POSTGRES_URL    = f"jdbc:postgresql://{POSTGRES_HOST}:5432/{POSTGRES_DB}"
POSTGRES_PROPS  = {"user": POSTGRES_USER, "password": POSTGRES_PASS, "driver": "org.postgresql.Driver"}

KNOWN_AS = {15169, 16509, 8075, 32934, 13335, 1299, 3356, 174, 2914, 7018,
            20940, 54113, 36040, 15133, 22822, 6939, 3257, 1273, 5511, 6453}

BGP_SCHEMA = StructType([
    StructField("timestamp",  StringType(),  True),
    StructField("router_id",  StringType(),  True),
    StructField("peer_as",    IntegerType(), True),
    StructField("origin_as",  IntegerType(), True),
    StructField("prefix",     StringType(),  True),
    StructField("event_type", StringType(),  True),
    StructField("as_path",    StringType(),  True),
    StructField("next_hop",   StringType(),  True),
    StructField("med",        IntegerType(), True),
    StructField("communities",StringType(),  True),
])

def write_to_postgres(df, epoch_id, table):
    try:
        df.write.jdbc(url=POSTGRES_URL, table=table, mode="append", properties=POSTGRES_PROPS)
        logger.info(f"Wrote batch {epoch_id} to {table}")
    except Exception as e:
        logger.error(f"Failed to write batch {epoch_id} to {table}: {e}")

def main():
    logger.info("Starting BGP Processor...")

    spark = SparkSession.builder \
        .appName("BGP-Route-Monitor") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/bgp-checkpoint") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created")

    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", "bgp_raw_events") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    parsed = raw.select(
        from_json(col("value").cast("string"), BGP_SCHEMA).alias("data"),
        col("timestamp").alias("kafka_ts")
    ).select("data.*", "kafka_ts") \
     .withColumn("event_time", col("kafka_ts"))

    # Write all raw events to routing_events table
    raw_query = parsed.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, eid: write_to_postgres(
            df.select(
                col("event_time").alias("timestamp"),
                col("router_id"), col("peer_as"), col("origin_as"),
                col("prefix"), col("event_type"), col("next_hop"),
                col("med")
            ), eid, "routing_events"
        )) \
        .option("checkpointLocation", "/tmp/bgp-raw-checkpoint") \
        .trigger(processingTime="10 seconds") \
        .start()

    logger.info("Raw BGP stream query started")

    # Route flap detection: >5 announce/withdraw per prefix in 60s tumbling window
    flap_df = parsed \
        .filter(col("event_type").isin("ANNOUNCE", "WITHDRAW")) \
        .withWatermark("event_time", "30 seconds") \
        .groupBy(window(col("event_time"), "60 seconds"), col("prefix")) \
        .agg(count("*").alias("event_count")) \
        .filter(col("event_count") > 5)

    flap_query = flap_df.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, eid: write_to_postgres(
            df.select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("prefix"),
                lit(None).cast("integer").alias("origin_as"),
                col("event_count"),
                lit("ROUTE_FLAP").alias("alert_type"),
                lit("HIGH").alias("severity"),
                current_timestamp().alias("detected_at")
            ), eid, "bgp_alerts"
        )) \
        .option("checkpointLocation", "/tmp/bgp-flap-checkpoint") \
        .trigger(processingTime="10 seconds") \
        .start()

    logger.info("BGP flap detection query started")

    # BGP hijack detection: origin_as not in known list
    hijack_df = parsed \
        .filter(col("event_type") == "ANNOUNCE") \
        .filter(~col("origin_as").isin(list(KNOWN_AS))) \
        .withWatermark("event_time", "30 seconds") \
        .groupBy(window(col("event_time"), "60 seconds"), col("prefix"), col("origin_as")) \
        .agg(count("*").alias("event_count"))

    hijack_query = hijack_df.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, eid: write_to_postgres(
            df.select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("prefix"),
                col("origin_as"),
                col("event_count"),
                lit("BGP_HIJACK").alias("alert_type"),
                lit("CRITICAL").alias("severity"),
                current_timestamp().alias("detected_at")
            ), eid, "bgp_alerts"
        )) \
        .option("checkpointLocation", "/tmp/bgp-hijack-checkpoint") \
        .trigger(processingTime="10 seconds") \
        .start()

    logger.info("BGP hijack detection query started")
    logger.info("All streaming queries running. Awaiting termination...")

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
