"""
DNS Stream Processor
====================
Reads DNS query events from Kafka topic 'dns_raw_events' using
Apache Spark Structured Streaming.

Applies three anomaly detection rules:
  1. NXDOMAIN Storm  : > 100 NXDOMAIN responses from same IP in 30s
  2. DNS Tunneling   : domain length > 50 OR entropy > 3.5
  3. Amplification   : high volume of ANY/TXT queries from same IP

Results are written to:
  - PostgreSQL table: dns_events   (all raw events)
  - PostgreSQL table: dns_alerts   (detected anomalies only)
"""

import sys
import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count,
    current_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, FloatType, TimestampType
)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [DNS-PROCESSOR] %(message)s',
    stream=sys.stdout,
    force=True
)
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP   = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
POSTGRES_HOST     = os.getenv('POSTGRES_HOST', 'postgres')
POSTGRES_DB       = os.getenv('POSTGRES_DB', 'bgp_dns_db')
POSTGRES_USER     = os.getenv('POSTGRES_USER', 'admin')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'admin123')
POSTGRES_URL      = f"jdbc:postgresql://{POSTGRES_HOST}:5432/{POSTGRES_DB}"

POSTGRES_PROPS = {
    "user"    : POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver"  : "org.postgresql.Driver"
}

# ── DNS Event Schema ──────────────────────────────────────────────────────────
DNS_SCHEMA = StructType([
    StructField("timestamp",      StringType(),  True),
    StructField("resolver_ip",    StringType(),  True),
    StructField("source_ip",      StringType(),  True),
    StructField("queried_domain", StringType(),  True),
    StructField("query_type",     StringType(),  True),
    StructField("response_code",  StringType(),  True),
    StructField("ttl",            IntegerType(), True),
    StructField("entropy",        FloatType(),   True),
    StructField("domain_length",  IntegerType(), True),
    StructField("anomaly",        StringType(),  True),
])


def create_spark_session() -> SparkSession:
    """Create Spark session with Kafka and PostgreSQL connectors."""
    log.info("Creating Spark session...")
    spark = SparkSession.builder \
        .appName("DNS-Route-Monitor") \
        .config("spark.jars", ",".join([
            "/opt/spark/jars/spark-sql-kafka.jar",
            "/opt/spark/jars/kafka-clients.jar",
            "/opt/spark/jars/spark-token-provider.jar",
            "/opt/spark/jars/commons-pool2.jar",
            "/opt/spark/jars/postgresql.jar",
        ])) \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/dns-checkpoint") \
        .config("spark.sql.shuffle.partitions", "3") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session created successfully")
    return spark


def write_to_postgres(df, epoch_id, table):
    """Write a micro-batch DataFrame to a PostgreSQL table."""
    if df.count() > 0:
        df.write \
            .jdbc(url=POSTGRES_URL,
                  table=table,
                  mode="append",
                  properties=POSTGRES_PROPS)
        log.info(f"Wrote {df.count()} rows to {table}")


def main():
    spark = create_spark_session()

    # ── Read stream from Kafka ────────────────────────────────────────────────
    log.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP}")
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", "dns_raw_events") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # ── Parse JSON events ─────────────────────────────────────────────────────
    parsed = raw_stream.select(
        from_json(
            col("value").cast("string"),
            DNS_SCHEMA
        ).alias("data")
    ).select("data.*") \
     .withColumn("event_time", col("timestamp").cast(TimestampType())) \
     .withWatermark("event_time", "15 seconds")

    log.info("Stream parsed. Applying DNS anomaly detection...")

    # ══════════════════════════════════════════════════════════════════════════
    # DETECTION RULE 1 — NXDOMAIN Storm
    # Count NXDOMAIN responses per source IP in a 30s sliding window (10s slide)
    # If count > 100 → NXDOMAIN storm detected
    # ══════════════════════════════════════════════════════════════════════════
    nxdomain_alerts = parsed \
        .filter(col("response_code") == "NXDOMAIN") \
        .groupBy(
            window(col("event_time"), "30 seconds", "10 seconds"),
            col("source_ip")
        ) \
        .agg(count("*").alias("nxdomain_count")) \
        .filter(col("nxdomain_count") > 100) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("source_ip"),
            col("nxdomain_count").alias("event_count"),
            lit("NXDOMAIN_STORM").alias("alert_type"),
            lit("HIGH").alias("severity"),
            current_timestamp().alias("detected_at")
        )

    # ══════════════════════════════════════════════════════════════════════════
    # DETECTION RULE 2 — DNS Tunneling
    # Domain length > 50 characters OR entropy score > 3.5
    # These indicate data being smuggled inside DNS queries
    # ══════════════════════════════════════════════════════════════════════════
    tunneling_alerts = parsed \
        .filter(
            (col("domain_length") > 50) |
            (col("entropy") > 3.5)
        ) \
        .select(
            current_timestamp().alias("window_start"),
            current_timestamp().alias("window_end"),
            col("source_ip"),
            lit(1).alias("event_count"),
            lit("DNS_TUNNELING").alias("alert_type"),
            lit("CRITICAL").alias("severity"),
            current_timestamp().alias("detected_at")
        )

    # ══════════════════════════════════════════════════════════════════════════
    # DETECTION RULE 3 — Amplification Attack
    # High volume of ANY/TXT queries from same source IP in 30s window
    # ══════════════════════════════════════════════════════════════════════════
    amplification_alerts = parsed \
        .filter(col("query_type").isin(["ANY", "TXT"])) \
        .groupBy(
            window(col("event_time"), "30 seconds", "10 seconds"),
            col("source_ip")
        ) \
        .agg(count("*").alias("query_count")) \
        .filter(col("query_count") > 50) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("source_ip"),
            col("query_count").alias("event_count"),
            lit("AMPLIFICATION").alias("alert_type"),
            lit("MEDIUM").alias("severity"),
            current_timestamp().alias("detected_at")
        )

    # ══════════════════════════════════════════════════════════════════════════
    # SINK 1 — Write ALL raw DNS events to PostgreSQL dns_events table
    # ══════════════════════════════════════════════════════════════════════════
    raw_query = parsed \
        .select(
            col("event_time").alias("timestamp"),
            col("source_ip"),
            col("resolver_ip"),
            col("queried_domain"),
            col("query_type"),
            col("response_code"),
            col("ttl"),
            col("entropy"),
            col("domain_length")
        ) \
        .writeStream \
        .foreachBatch(lambda df, eid: write_to_postgres(df, eid, "dns_events")) \
        .option("checkpointLocation", "/tmp/dns-raw-checkpoint") \
        .trigger(processingTime="10 seconds") \
        .start()

    # ══════════════════════════════════════════════════════════════════════════
    # SINK 2 — Write all DNS alerts to PostgreSQL dns_alerts table
    # ══════════════════════════════════════════════════════════════════════════
    nxdomain_query = nxdomain_alerts \
        .writeStream \
        .foreachBatch(lambda df, eid: write_to_postgres(df, eid, "dns_alerts")) \
        .option("checkpointLocation", "/tmp/dns-nxdomain-checkpoint") \
        .outputMode("update") \
        .trigger(processingTime="10 seconds") \
        .start()

    tunneling_query = tunneling_alerts \
        .writeStream \
        .foreachBatch(lambda df, eid: write_to_postgres(df, eid, "dns_alerts")) \
        .option("checkpointLocation", "/tmp/dns-tunneling-checkpoint") \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()

    amplification_query = amplification_alerts \
        .writeStream \
        .foreachBatch(lambda df, eid: write_to_postgres(df, eid, "dns_alerts")) \
        .option("checkpointLocation", "/tmp/dns-amplification-checkpoint") \
        .outputMode("update") \
        .trigger(processingTime="10 seconds") \
        .start()

    log.info("All DNS streaming queries started. Waiting for data...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()