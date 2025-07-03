import argparse
import json
import logging
import uuid

import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
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
    max as spark_max
)
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pyspark.sql.window import Window

# --- UDF for Consistent UUID Generation ---
# ใช้ Namespace คงที่เพื่อให้ได้ UUIDv5 ที่เหมือนเดิมสำหรับ license_plate เดิม
NAMESPACE_UUID = uuid.uuid4()

def generate_uuid5(license_plate):
    """Generates a consistent UUIDv5 for a given license plate."""
    return str(uuid.uuid5(NAMESPACE_UUID, license_plate))

uuid_udf = udf(generate_uuid5, StringType())

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

    return parser.parse_args()


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

                if camera_ids and area_name and area_id:
                    areas.append({
                        "area_id": area_id,
                        "area_name": area_name,
                        "camera_ids": camera_ids,
                        "required_camera_count": len(camera_ids)
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
        StructField("required_camera_count", StringType(), False)
    ])
    areas_df = spark.createDataFrame(areas_list, schema=area_schema)

    window_spec = Window.partitionBy("license_plate", "camera_id").orderBy(col("event_time").desc())
    unique_events_df = (
        events_df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
        .withColumn("record_uuid", uuid_udf(col("license_plate")))
    ).cache()

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
    ).cache()  # *** FIX: เพิ่ม cache ***

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
    ).cache()  # *** FIX: รักษา cache ***

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
    ).cache()  # *** FIX: เพิ่ม cache ***

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
    # --- Load Data from PostgreSQL ---
    dbtable_query = (
        f"(SELECT * FROM {args.postgres_table} "
        f"WHERE event_time >= NOW() - INTERVAL '{args.lookback_hours} hours') AS filtered_data"
    )
    logger.info(f"Querying last {args.lookback_hours} hours from table {args.postgres_table}.")
    
    jdbc_url = f"jdbc:postgresql://{args.postgres_host}:{args.postgres_port}/{args.postgres_db}"

    try:
        df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", dbtable_query)
            .option("user", args.postgres_user)
            .option("password", args.postgres_password)
            .option("driver", "org.postgresql.Driver")
            .option("fetchsize", "10000")
            .option("numPartitions", "8") # Can be tuned
            .load()
        ).cache()
        logger.info(f"Loaded {df.count()} records from PostgreSQL.")
    except Exception as e:
        logger.error(f"Failed to load data from PostgreSQL: {e}", exc_info=True)
        spark.stop()
        return

    # --- Load Area Configs from Redis ---
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
        df.unpersist()
        spark.stop()
        return
        
    all_areas = get_all_areas_from_redis(r)

    # --- Process Data ---
    alerts_df, logs_df = process_vehicle_intersections(
        spark, df, all_areas, args.time_threshold_seconds, processing_timestamp
    )
    df.unpersist()
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
            alert_count = alerts_df.count()
            logger.info(f"Sending {alert_count} area alerts to Kafka topic '{args.kafka_alerts_topic}'.")
            (alerts_df.select(to_json(struct("*")).alias("value"))
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
