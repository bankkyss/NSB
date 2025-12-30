import argparse
import json
import logging
import uuid
from datetime import datetime, timedelta

import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    broadcast,
    col,
    collect_list,
    countDistinct,
    current_timestamp,
    explode,
    lit,
    row_number,
    struct,
    to_json,
    udf,
    unix_timestamp,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    when,
    hour
)
from pyspark.sql.types import ArrayType, StringType, StructField, StructType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from pyspark.storagelevel import StorageLevel

# --- UDF for Consistent UUID Generation ---
# ใช้ Namespace คงที่เพื่อให้ได้ UUIDv5 ที่เหมือนเดิมสำหรับ license_plate เดิม
NAMESPACE_UUID = uuid.uuid4()

def generate_uuid5(license_plate):
    """Generates a consistent UUIDv5 for a given license plate."""
    return str(uuid.uuid5(NAMESPACE_UUID, license_plate))

uuid_udf = udf(generate_uuid5, StringType())

CACHE_LEVEL = StorageLevel.MEMORY_AND_DISK

def parse_arguments():
    """
    Parses all command-line arguments passed from the Airflow DAG.
    This is the central point of configuration for the script.
    """
    parser = argparse.ArgumentParser(description="Vehicle Area Analysis Spark Job")

    # --- PostgreSQL Arguments ---
    parser.add_argument("--postgres-host", required=True, help="PostgreSQL host")
    parser.add_argument("--postgres-port", required=True, help="PostgreSQL port")
    parser.add_argument("--postgres-db", required=True, help="PostgreSQL database name")
    parser.add_argument("--postgres-user", required=True, help="PostgreSQL username")
    parser.add_argument("--postgres-password", required=True, help="PostgreSQL password")
    parser.add_argument("--postgres-table", default="vehicle_events", help="Table name for vehicle events")

    # --- Redis Arguments ---
    parser.add_argument("--redis-host", required=True, help="Redis host")
    parser.add_argument("--redis-port", required=True, type=int, help="Redis port")
    parser.add_argument("--redis-password", required=True, help="Redis password")

    # --- Kafka Arguments ---
    parser.add_argument("--kafka-brokers", required=True, help="Kafka bootstrap servers")
    parser.add_argument("--kafka-alerts-topic", required=True, help="Kafka topic for area alerts")
    parser.add_argument("--kafka-log-event-topic", required=True, help="Kafka topic for detailed event logs")

    # --- Job Parameter Arguments ---
    parser.add_argument("--lookback-hours", type=int, default=12, help="How many hours back to query data from PostgreSQL")
    parser.add_argument("--time-threshold-seconds", type=int, default=300, help="Time window in seconds to consider events for an area detection")
    parser.add_argument("--parquet-cache-path", default="", help="Parquet cache path (e.g., rustfs mount). When set, only incremental data is loaded from PostgreSQL")
    parser.add_argument("--redis-checkpoint-key", default="vehicle_area_analysis:checkpoint_ts", help="Redis key for the last processed event_time checkpoint")
    parser.add_argument("--checkpoint-overlap-seconds", type=int, default=60, help="Overlap window in seconds when querying from the checkpoint to avoid late events")

    return parser.parse_args()


def format_timestamp(ts):
    if isinstance(ts, datetime):
        return ts.isoformat(sep=" ", timespec="seconds")
    return str(ts) if ts is not None else None


def clamp_checkpoint(checkpoint_ts, processing_ts, logger, label="Checkpoint"):
    if checkpoint_ts and processing_ts and checkpoint_ts > processing_ts:
        drift_seconds = (checkpoint_ts - processing_ts).total_seconds()
        logger.warning(
            "%s is ahead of processing timestamp by %.0fs; clamping to processing timestamp.",
            label,
            drift_seconds,
        )
        return processing_ts
    return checkpoint_ts


def parse_checkpoint(raw_value, logger):
    if not raw_value:
        return None
    try:
        return datetime.fromisoformat(raw_value)
    except (TypeError, ValueError) as exc:
        logger.warning(f"Invalid checkpoint value '{raw_value}': {exc}. Ignoring.")
        return None


def get_checkpoint_timestamp(redis_client, key, logger):
    if not key:
        return None
    try:
        raw_value = redis_client.get(key)
        logger.info("Checkpoint raw value from Redis key '%s': %s", key, raw_value)
    except redis.exceptions.RedisError as exc:
        logger.warning(f"Failed to read checkpoint from Redis key '{key}': {exc}")
        return None
    return parse_checkpoint(raw_value, logger)


def update_checkpoint_timestamp(redis_client, key, new_timestamp, logger):
    if not key or new_timestamp is None:
        return
    new_value = format_timestamp(new_timestamp)
    if not new_value:
        return
    try:
        existing_raw = redis_client.get(key)
        existing_ts = parse_checkpoint(existing_raw, logger) if existing_raw else None
        if existing_ts and existing_ts >= new_timestamp:
            logger.info(f"Checkpoint unchanged at {existing_raw}.")
            return
        redis_client.set(key, new_value)
        logger.info(f"Updated checkpoint to {new_value}.")
    except redis.exceptions.RedisError as exc:
        logger.warning(f"Failed to update checkpoint in Redis key '{key}': {exc}")


def load_cached_events(spark, cache_path, min_event_time, logger):
    if not cache_path:
        return None
    logger.info(
        "Loading parquet cache from %s (min_event_time=%s).",
        cache_path,
        format_timestamp(min_event_time),
    )
    try:
        cached_df = spark.read.parquet(cache_path)
    except AnalysisException as exc:
        logger.warning(f"No parquet cache found at {cache_path}: {exc}")
        return None
    except Exception as exc:
        logger.error(f"Failed to read parquet cache from {cache_path}: {exc}", exc_info=True)
        return None
    logger.info("Loaded parquet cache from %s.", cache_path)
    logger.info("Parquet cache schema: %s", cached_df.schema.simpleString())
    if min_event_time is not None:
        cached_df = cached_df.filter(col("event_time") >= lit(min_event_time))
    return cached_df


def log_event_date_stats(events_df, logger, label):
    if "event_date" not in events_df.columns:
        logger.warning("%s missing event_date column; parquet partitions may be empty.", label)
        return
    stats = (
        events_df.agg(
            spark_sum(when(col("event_date").isNull(), 1).otherwise(0)).alias("null_event_date"),
            spark_min("event_date").alias("min_event_date"),
            spark_max("event_date").alias("max_event_date"),
        ).collect()[0]
    )
    logger.info(
        "%s event_date stats: null=%s min=%s max=%s",
        label,
        stats["null_event_date"],
        format_timestamp(stats["min_event_date"]),
        format_timestamp(stats["max_event_date"]),
    )


def log_cache_listing(spark, cache_path, logger, max_entries=20):
    try:
        jconf = spark._jsc.hadoopConfiguration()
        jpath = spark._jvm.org.apache.hadoop.fs.Path(cache_path)
        fs = jpath.getFileSystem(jconf)
        if not fs.exists(jpath):
            logger.warning("Cache path does not exist for listing: %s", cache_path)
            return
        it = fs.listFiles(jpath, True)
        count = 0
        while it.hasNext() and count < max_entries:
            status = it.next()
            logger.info(
                "Cache object: %s (size=%d)",
                status.getPath().toString(),
                status.getLen(),
            )
            count += 1
        if count == 0:
            logger.warning("Cache path has no files: %s", cache_path)
        elif it.hasNext():
            logger.info("Cache listing truncated after %d entries.", max_entries)
    except Exception as exc:
        logger.warning("Failed to list cache path %s: %s", cache_path, exc)


def get_all_areas_from_redis(redis_client, pattern="area_detect:*"):
    """
    Scans Redis for all area configurations using the specified pattern.
    """
    logger = logging.getLogger(__name__)
    areas = []
    logger.info(f"Scanning Redis for keys matching pattern: '{pattern}'")
    try:
        for key in redis_client.scan_iter(match=pattern, count=1000):
            try:
                payload_str = redis_client.get(key)
                if not payload_str:
                    logger.warning(f"Key '{key}' has no payload. Skipping.")
                    continue

                payload = json.loads(payload_str)
                area_name = payload.get("name")
                area_id = payload.get("id")
                camera_ids = payload.get("camera_id", [])
                number_of_camera = payload.get("number_of_camera", len(camera_ids))
                if camera_ids and area_name and area_id:
                    areas.append({
                        "area_id": area_id,
                        "area_name": area_name,
                        "camera_ids": camera_ids,
                        "required_camera_count": number_of_camera
                    })
                else:
                    logger.warning(f"Incomplete data for key '{key}'. Skipping.")

            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON for key {key}: {e}")
            except Exception as e:
                logger.error(f"Error processing key {key}: {e}")
    except redis.exceptions.RedisError as e:
        logger.error(f"Redis connection error: {e}")
        return []

    logger.info(f"Successfully loaded {len(areas)} area configurations from Redis.")
    return areas

def process_vehicle_intersections(spark, events_df, areas_list, time_threshold, processing_ts):
    """
    Core processing logic that strictly derives logs from the created alerts.
    Workflow: Detections -> alerts_df -> Extract UUIDs -> Query for logs_df.
    """
    logger = logging.getLogger(__name__)

    # --- ขั้นตอนที่ 1: เตรียมข้อมูลเบื้องต้น ---
    area_schema = StructType([
        StructField("area_id", StringType(), False),
        StructField("area_name", StringType(), False),
        StructField("camera_ids", ArrayType(StringType()), False),
        # StructField("required_camera_count", StringType(), False)
        StructField("required_camera_count", IntegerType(), False) 
    ])
    areas_df = spark.createDataFrame(areas_list, schema=area_schema)
    
    # --- FIX OPTIMIZATION: กรองเฉพาะรถที่มีความเคลื่อนไหวในช่วงเวลา threshold ---
    # ถ้าไม่มี event ในช่วง 5-10 นาทีนี้ ก็ไม่มีทาง trigger alert ได้
    # เราจึงหา "Active Cars" ก่อน แล้วค่อยเอาไป Join
    recent_active_start = processing_ts - timedelta(seconds=time_threshold)
    logger.info(f"Filtering for active cars with events since {format_timestamp(recent_active_start)} (threshold {time_threshold}s)")
    
    # 1. หา License Plate ของรถที่ Active
    active_cars_df = (
        events_df
        .filter(col("event_time") >= lit(recent_active_start))
        .select("license_plate")
        .distinct()
    )
    
    # 2. กรอง Events ทั้งหมด (Lookback 12hr) ให้เหลือเฉพาะของ Active Cars
    # ใช้ broadcast join เพราะ active_cars_df น่าจะมีขนาดเล็ก
    filtered_events_df = events_df.join(broadcast(active_cars_df), "license_plate", "inner")

    window_spec = Window.partitionBy("license_plate", "camera_id").orderBy(col("event_time").desc())
    unique_events_df = (
        filtered_events_df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
        .withColumn("record_uuid", uuid_udf(col("license_plate")))
    ).persist(CACHE_LEVEL)

    # --- ขั้นตอนที่ 2: สร้าง alerts_df ---
    areas_exploded_df = areas_df.withColumn("camera_id", explode(col("camera_ids")))
    events_with_areas_df = unique_events_df.join(areas_exploded_df, on="camera_id", how="inner")

    all_event_columns = [c for c in unique_events_df.columns]
    area_plate_aggregates_df = (
        events_with_areas_df
        .groupBy("area_id", "area_name", "license_plate", "required_camera_count")
        .agg(
            countDistinct("camera_id").alias("seen_camera_count"),
            spark_max("event_time").alias("latest_event_time"),
            collect_list("camera_id").alias("cameras"),
            collect_list("car_id").alias("car_id_list"),
            collect_list("province").alias("province_list"),
            collect_list("vehicle_brand").alias("vehicle_brand_list"),
            collect_list("vehicle_brand_model").alias("vehicle_brand_model_list"),
            collect_list("vehicle_color").alias("vehicle_color_list"),
            collect_list("vehicle_body_type").alias("vehicle_body_type_list"),
            collect_list("vehicle_brand_year").alias("vehicle_brand_year_list"),
            collect_list("camera_name").alias("camera_name_list"),
            collect_list("event_time").alias("event_time_list"),
            collect_list("event_date").alias("event_date_list"),
            collect_list("gps_latitude").alias("gps_latitude_list"),
            collect_list("gps_longitude").alias("gps_longitude_list"),
            collect_list("created_at").alias("created_at_list"),
            collect_list("record_uuid").alias("record_uuid_list"),
            collect_list(struct(*all_event_columns)).alias("events_data")
        )
    )

    potential_detections_df = area_plate_aggregates_df.filter(col("seen_camera_count") >= col("required_camera_count"))
    
    # *** FIX: ใช้ timestamp เดียวกันตลอด ***
    processing_ts_lit = lit(processing_ts)
    detections_df = potential_detections_df.filter(
        (unix_timestamp(processing_ts_lit) - unix_timestamp(col("latest_event_time"))) <= time_threshold
    ).persist(CACHE_LEVEL)  # *** FIX: เพิ่ม cache ***

    # Count detections ก่อน transform
    detections_count = detections_df.count()
    logger.info(f"Found {detections_count} detections after filtering.")

    alerts_df = (
        detections_df
        .withColumn("event_type", lit("area_detection"))
        .select(
            "license_plate",
            "cameras",
            "car_id_list",
            "province_list", 
            "vehicle_brand_list",
            "vehicle_brand_model_list",
            "vehicle_color_list",
            "vehicle_body_type_list",
            "vehicle_brand_year_list",
            "camera_name_list",
            "event_time_list",
            "event_date_list",
            "gps_latitude_list",
            "gps_longitude_list",
            "created_at_list",
            "record_uuid_list",
            "area_name",
            "area_id",
            "event_type",
            "events_data"
        )
    ).persist(CACHE_LEVEL)  # *** FIX: รักษา cache ***

    # *** FIX: Count alerts ทันทีหลัง cache ***
    alerts_count = alerts_df.count()
    logger.info(f"Successfully created {alerts_count} alerts.")

    # --- ขั้นตอนที่ 3: สร้าง logs_df จาก cached detections_df ---
    events_with_area_info_df = (
        detections_df.select("area_id", "area_name", explode("events_data").alias("event_struct"))
        .select("area_id", "area_name", "event_struct.*")
    )
    
    # ดึง UUID ทั้งหมดจาก alerts_df ที่ cache แล้ว
    uuids_to_keep_df = (
        alerts_df
        .select(explode(col("record_uuid_list")).alias("record_uuid"))
        .distinct()
    )

    logs_df = (
        events_with_area_info_df
        .join(uuids_to_keep_df, on="record_uuid", how="inner")
        .withColumn("event_type", lit("area_detection"))
    ).persist(CACHE_LEVEL)  # *** FIX: เพิ่ม cache ***

    # *** FIX: Count logs ทันทีหลัง cache ***
    logs_count = logs_df.count()
    logger.info(f"Derived {logs_count} logs from the created alerts.")

    # เคลียร์ cache ที่ไม่ใช้แล้ว
    unique_events_df.unpersist()
    detections_df.unpersist()

    return alerts_df, logs_df


def main():
    """Main execution function."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    args = parse_arguments()

    spark = (
        SparkSession.builder
        .appName("VehicleAreaAnalysisProduction")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark Session created. Running with arguments: {vars(args)}")
    processing_timestamp = spark.sql("SELECT current_timestamp()").collect()[0][0]
    lookback_start = processing_timestamp - timedelta(hours=args.lookback_hours)
    logger.info("Processing timestamp: %s", format_timestamp(processing_timestamp))
    logger.info("Lookback start timestamp: %s", format_timestamp(lookback_start))
    cache_enabled = bool(args.parquet_cache_path)

    # --- Load Area Configs from Redis (also used for checkpoint) ---
    try:
        r = redis.Redis(
            host=args.redis_host,
            port=args.redis_port,
            password=args.redis_password,
            decode_responses=True,
            socket_connect_timeout=5
        )
        r.ping()
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}", exc_info=True)
        spark.stop()
        return

    checkpoint_ts = None
    if cache_enabled:
        checkpoint_ts = get_checkpoint_timestamp(r, args.redis_checkpoint_key, logger)
        if checkpoint_ts:
            delta_seconds = (processing_timestamp - checkpoint_ts).total_seconds()
            logger.info(
                "Checkpoint timestamp: %s (delta vs processing timestamp: %.0fs)",
                format_timestamp(checkpoint_ts),
                delta_seconds,
            )
        checkpoint_ts = clamp_checkpoint(checkpoint_ts, processing_timestamp, logger)

    query_start_ts = lookback_start
    if cache_enabled and checkpoint_ts:
        overlap = max(args.checkpoint_overlap_seconds, 0)
        query_start_ts = checkpoint_ts - timedelta(seconds=overlap)
        if query_start_ts < lookback_start:
            query_start_ts = lookback_start
    logger.info("Query start timestamp (after checkpoint/overlap): %s", format_timestamp(query_start_ts))

    # --- Load Data from PostgreSQL ---
    query_start_str = format_timestamp(query_start_ts)
    dbtable_query = (
        f"(SELECT * FROM {args.postgres_table} "
        f"WHERE event_time >= '{query_start_str}') AS filtered_data"
    )
    logger.info(
        "Querying PostgreSQL from %s (lookback %s hours, cache %s).",
        query_start_str,
        args.lookback_hours,
        "enabled" if cache_enabled else "disabled",
    )

    jdbc_url = f"jdbc:postgresql://{args.postgres_host}:{args.postgres_port}/{args.postgres_db}"

    try:
        new_events_df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", dbtable_query)
            .option("user", args.postgres_user)
            .option("password", args.postgres_password)
            .option("driver", "org.postgresql.Driver")
            .option("fetchsize", "10000")
            .option("numPartitions", "8") # Can be tuned
            .option("application_name", 'vehicle_area_analysis')
            .load()
            .withColumn("event_hour", hour("event_time"))
        ).persist(CACHE_LEVEL)
        new_events_count = new_events_df.count()
        logger.info(f"Loaded {new_events_count} records from PostgreSQL.")
        log_event_date_stats(new_events_df, logger, "postgres_events")
    except Exception as e:
        logger.error(f"Failed to load data from PostgreSQL: {e}", exc_info=True)
        spark.stop()
        return
        
    all_areas = get_all_areas_from_redis(r)

    cached_df = None
    if cache_enabled:
        cached_df = load_cached_events(spark, args.parquet_cache_path, lookback_start, logger)
        if cached_df is not None:
            cached_count = cached_df.count()
            logger.info(f"Loaded {cached_count} records from parquet cache.")
            log_event_date_stats(cached_df, logger, "cached_events")

    if cached_df is not None:
        events_df = cached_df.unionByName(new_events_df, allowMissingColumns=True)
        logger.info("Using cached parquet data; unioned with PostgreSQL data.")
    else:
        events_df = new_events_df
        logger.info("No cached parquet data loaded; using only PostgreSQL data.")

    events_df = events_df.filter(col("event_time") >= lit(lookback_start)).persist(CACHE_LEVEL)

    if cache_enabled and new_events_count > 0:
        try:
            # บังคับ materialize DataFrame ก่อนเขียน
            df_to_cache = new_events_df.select("*")
            row_count = df_to_cache.count()
            logger.info(f"Materializing {row_count} rows for parquet cache write.")
            
            # เขียน parquet โดยใช้ Hadoop committer แบบ classic พร้อม partition by date
            (df_to_cache
                .write
                .format("parquet")
                .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "true")
                .mode("append")
                .partitionBy("event_date", "event_hour")
                .save(args.parquet_cache_path))
            
            logger.info(f"Appended {new_events_count} records to parquet cache at {args.parquet_cache_path}.")
            log_cache_listing(spark, args.parquet_cache_path, logger)
            max_event_time = (
                new_events_df.agg(spark_max("event_time").alias("max_event_time"))
                .collect()[0]["max_event_time"]
            )
            safe_checkpoint = clamp_checkpoint(max_event_time, processing_timestamp, logger, label="Max event_time")
            update_checkpoint_timestamp(r, args.redis_checkpoint_key, safe_checkpoint, logger)
        except Exception as e:
            logger.error(f"Failed to update parquet cache at {args.parquet_cache_path}: {e}", exc_info=True)

    # --- Process Data ---
    alerts_df, logs_df = process_vehicle_intersections(
        spark, events_df, all_areas, args.time_threshold_seconds, processing_timestamp
    )
    events_df.unpersist()
    new_events_df.unpersist()
    # --- Send Results to Kafka ---
    # Send detailed logs
    if not logs_df.rdd.isEmpty():
        try:
            log_count = logs_df.count()
            logger.info(f"Sending {log_count} detailed event logs to Kafka topic '{args.kafka_log_event_topic}'.")
            (logs_df.select(to_json(struct("*")).alias("value"))
                .write
                .format("kafka")
                .option("kafka.bootstrap.servers", args.kafka_brokers)
                .option("topic", args.kafka_log_event_topic)
                .save())
            logger.info("Log data sent successfully.")
        except Exception as e:
            logger.error(f"Kafka send error for logs_df: {e}", exc_info=True)
    else:
        logger.info("No detailed event logs to send to Kafka.")

    # Send aggregated alerts
    if not alerts_df.rdd.isEmpty():
        try:
            # สร้าง DataFrame ใหม่ที่ไม่มีคอลัมน์ "events_data"
            alerts_to_send_df = alerts_df.drop("events_data")

            alert_count = alerts_to_send_df.count() # นับจาก DF ที่จะส่งจริง
            logger.info(f"Sending {alert_count} area alerts to Kafka topic '{args.kafka_alerts_topic}'.")

            (alerts_to_send_df.select(to_json(struct("*")).alias("value")) # ใช้ struct("*") กับ DF ที่ไม่มี events_data แล้ว
                .write
                .format("kafka")
                .option("kafka.bootstrap.servers", args.kafka_brokers)
                .option("topic", args.kafka_alerts_topic)
                .save())
            logger.info("Alert data sent successfully.")
        except Exception as e:
            logger.error(f"Kafka send error for alerts_df: {e}", exc_info=True)
    else:
        logger.info("No area alerts to send to Kafka.")

    logger.info("Processing complete. Stopping Spark session.")
    spark.stop()


if __name__ == '__main__':
    main()
