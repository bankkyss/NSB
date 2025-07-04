import logging
import uuid
import redis
import json
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, countDistinct, abs, collect_list, size,
    to_json, struct, unix_timestamp, max as spark_max,
    current_timestamp, explode, lit, date_format, to_timestamp
)
from graphframes import GraphFrame # Ensure GraphFrame is available

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def parse_arguments():
    """Parse command line arguments from DAG"""
    parser = argparse.ArgumentParser(description='Vehicle Graph Analysis with Spark')
    
    # PostgreSQL parameters
    parser.add_argument('--postgres-host', type=str, required=True, help='PostgreSQL host')
    parser.add_argument('--postgres-port', type=str, default='5432', help='PostgreSQL port')
    parser.add_argument('--postgres-db', type=str, required=True, help='PostgreSQL database name')
    parser.add_argument('--postgres-user', type=str, required=True, help='PostgreSQL username')
    parser.add_argument('--postgres-password', type=str, required=True, help='PostgreSQL password')
    parser.add_argument('--postgres-table', type=str, default='vehicle_events', help='PostgreSQL table name')
    
    # Kafka parameters
    parser.add_argument('--kafka-brokers', type=str, required=True, help='Kafka brokers')
    parser.add_argument('--kafka-alerts-topic', type=str, default='alerts_topic', help='Kafka alerts topic')
    # ---- ADD THIS LINE BACK ----
    parser.add_argument('--kafka-log-event-topic', type=str, default='log_event_topic', help='Kafka log event topic (accepted for compatibility, but not used)')
    
    # Analysis parameters
    parser.add_argument('--lookback-hours', type=int, default=24, help='Hours to look back for data')
    parser.add_argument('--time-threshold-seconds', type=int, default=300, help='Time threshold in seconds')
    
    # Redis parameters
    parser.add_argument('--redis-host', type=str, default='redis-primary', help='Redis host')
    parser.add_argument('--redis-port', type=int, default=6379, help='Redis port')
    parser.add_argument('--redis-password', type=str, default='my_password', help='Redis password')
    parser.add_argument('--redis-pattern', type=str, default='petternrecognition:*', help='Redis pattern for keys')
    
    return parser.parse_args()

def create_spark_session():
    """Create SparkSession with HDFS configurations for distributed environment"""
    try:
        active_session = SparkSession.getActiveSession()
        if active_session:
            logger.info("Stopping existing SparkSession...")
            active_session.stop()
    except Exception as e:
        logger.warning(f"Error stopping existing SparkSession: {e}")

    spark_builder = (
        SparkSession.builder
        .appName("Vehicle Graph Analysis (Alert Generation)")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-hadoop-hdfs-nn.speak-test.svc.cluster.local:9000")
        .config("spark.sql.warehouse.dir", "hdfs://hadoop-hadoop-hdfs-nn.speak-test.svc.cluster.local:9000/user/spark/warehouse")
    )

    spark = spark_builder.getOrCreate()
    logger.info("SparkSession created.")

    checkpoint_base_dir_hdfs = "hdfs://hadoop-hadoop-hdfs-nn.speak-test.svc.cluster.local:9000/spark-checkpoints/graphframes"
    checkpoint_dir_hdfs = f"{checkpoint_base_dir_hdfs}/{uuid.uuid4()}"

    try:
        spark.sparkContext.setCheckpointDir(checkpoint_dir_hdfs)
        logger.info(f"GraphFrames checkpoint directory set to HDFS: {checkpoint_dir_hdfs}")
    except Exception as e:
        logger.warning(f"Failed to set HDFS checkpoint directory: {e}. Falling back to local.")
        local_fallback_checkpoint_dir = f"/tmp/spark-checkpoints/graphframes-fallback/{uuid.uuid4()}"
        spark.sparkContext.setCheckpointDir(local_fallback_checkpoint_dir)
        logger.info(f"Using local checkpoint directory: {local_fallback_checkpoint_dir}")

    spark.sparkContext.setLogLevel("WARN")
    return spark

def get_rule_data(redis_host, redis_port, redis_password, redis_pattern):
    """Get rule data from Redis"""
    try:
        redis_client = redis.Redis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)
        rules = []
        # Add default rule
        rules.append({"rule_id": "default_rule", "name": "default_rule", "number_camera": 3, "time_range": 180, "camera_ids": []})
        
        logger.info(f"Scanning Redis for keys matching pattern: '{redis_pattern}'")
        for key in redis_client.scan_iter(match=redis_pattern, count=1000):
            try:
                payload_str = redis_client.get(key)
                if not payload_str: continue
                payload = json.loads(payload_str)
                if all(k in payload for k in ["id", "name", "number_camera", "time_range", "camera_id"]):
                    rules.append({
                        "rule_id": payload["id"], "name": payload["name"],
                        "number_camera": payload["number_camera"], "time_range": payload["time_range"],
                        "camera_ids": payload["camera_id"]
                    })
                else:
                    logger.warning(f"Incomplete data for key '{key}'. Skipping.")
            except (json.JSONDecodeError, Exception) as e:
                logger.error(f"Error processing key {key}: {e}")
    except redis.exceptions.RedisError as e:
        logger.error(f"Redis connection error: {e}")
        return []
    logger.info(f"Successfully loaded {len(rules)} pattern configurations from Redis.")
    return rules

def send_to_kafka(spark, df, kafka_brokers, kafka_topic):
    """Send DataFrame to Kafka topic"""
    if df.rdd.isEmpty():
        logger.info(f"DataFrame is empty. Nothing to send to Kafka topic '{kafka_topic}'.")
        return
    try:
        logger.info(f"Attempting to send data to Kafka topic '{kafka_topic}'...")
        kafka_output_df = df.select(to_json(struct("*")).alias("value"))
        (kafka_output_df.write
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_brokers)
            .option("topic", kafka_topic)
            .save())
        logger.info(f"Successfully sent data to Kafka topic '{kafka_topic}'.")
    except Exception as kafka_e:
        logger.error(f"Error sending data to Kafka: {str(kafka_e)}")

def main():
    """Main execution function"""
    args = parse_arguments()
    logger.info(f"Received parameters: PG={args.postgres_host}, Kafka={args.kafka_brokers}, Lookback={args.lookback_hours}h")
    
    spark = None
    try:
        spark = create_spark_session()
        jdbc_url = f"jdbc:postgresql://{args.postgres_host}:{args.postgres_port}/{args.postgres_db}"
        list_data = get_rule_data(args.redis_host, args.redis_port, args.redis_password, args.redis_pattern)
        
        for rule in list_data:
            rule_id, name, number_camera, time_range, camera_ids = rule.values()
            
            camera_filter = ""
            if camera_ids:
                camera_ids_sql_format = ", ".join([f"'{cam_id}'" for cam_id in camera_ids])
                camera_filter = f" AND camera_id IN ({camera_ids_sql_format})"

            dbtable_query = (
                f"(SELECT car_id, license_plate, province, vehicle_brand, vehicle_brand_model, "
                f"vehicle_color, vehicle_body_type, vehicle_brand_year, camera_name, camera_id, "
                f"event_time, event_date, gps_latitude, gps_longitude, created_at "
                f" FROM {args.postgres_table} "
                f" WHERE event_time >= NOW() - INTERVAL '{args.lookback_hours} hours'{camera_filter}"
                f") AS filtered_data"
            )
            logger.info(f"Executing query for rule '{name}' with {args.lookback_hours}h lookback.")

            try:
                # Load full data for alert creation
                df_full = (
                    spark.read.format("jdbc")
                    .option("url", jdbc_url)
                    .option("dbtable", dbtable_query)
                    .option("user", args.postgres_user)
                    .option("password", args.postgres_password)
                    .option("driver", "org.postgresql.Driver")
                    .option("fetchsize", "10000")
                    .option("numPartitions", "8")
                    .load()
                )
                df_full.cache()
                
                record_count = df_full.count()
                if record_count == 0:
                    logger.warning(f"No data found for rule '{name}'. Skipping.")
                    continue
                logger.info(f"Loaded {record_count} records for rule '{name}'.")

                # Prepare simplified DataFrame for graph processing
                df = df_full.select("license_plate", "camera_name", "event_time") \
                            .withColumn("timestamp_utc", unix_timestamp(col("event_time")))

                # Find vehicles with recent activity (vehicles of interest)
                current_time_unix = unix_timestamp(current_timestamp())
                df_interest = (
                    df.groupBy("license_plate")
                    .agg(spark_max("timestamp_utc").alias("latest_event_unix"))
                    .filter((current_time_unix - col("latest_event_unix")) <= args.time_threshold_seconds)
                    .select("license_plate")
                ).cache()
                
                interest_count = df_interest.count()
                if interest_count == 0:
                    logger.info(f"No recently active vehicles (within {args.time_threshold_seconds}s) for rule '{name}'. Skipping.")
                    continue
                logger.info(f"Found {interest_count} license plates with recent activity for rule '{name}'.")

            except Exception as e:
                logger.error(f"Failed to load/process data from PostgreSQL for rule '{name}': {str(e)}")
                continue

            try:
                # --- Graph Analysis (largely unchanged) ---
                events = df.withColumnRenamed("license_plate", "vehicle") \
                           .withColumnRenamed("camera_name", "point") \
                           .withColumnRenamed("timestamp_utc", "timestamp")
                events.cache()

                e1, e2 = events.alias("a"), events.alias("b")
                raw_edges = e1.join(e2,
                                    (col("a.point") == col("b.point")) &
                                    (col("a.vehicle") < col("b.vehicle")) &
                                    (abs(col("a.timestamp") - col("b.timestamp")) <= time_range)) \
                               .select(col("a.vehicle").alias("src"), col("b.vehicle").alias("dst"), col("a.point"))
                
                edges = raw_edges.groupBy("src", "dst") \
                                 .agg(countDistinct("point").alias("common_points")) \
                                 .filter(col("common_points") >= number_camera)
                edges.cache()

                if edges.rdd.isEmpty():
                    logger.warning(f"No edges met the criteria for rule '{name}'. No groups to process.")
                    continue

                verts = events.select(col("vehicle").alias("id")).distinct()
                g = GraphFrame(verts, edges.select("src", "dst"))
                
                cc = g.connectedComponents()
                
                groups_all = cc.groupBy("component") \
                               .agg(collect_list("id").alias("vehicles")) \
                               .filter(size(col("vehicles")) > 1)

                # Filter for groups containing at least one vehicle of interest
                groups_exploded = groups_all.select(col("component"), explode(col("vehicles")).alias("vehicle"))
                groups_with_interest = groups_exploded.join(df_interest.withColumnRenamed("license_plate", "vehicle"), "vehicle", "inner") \
                                                      .select("component").distinct() \
                                                      .join(groups_all, "component")
                
                num_groups_with_multiple = groups_with_interest.count()
                if num_groups_with_multiple == 0:
                    logger.info(f"No groups containing vehicles of interest were found for rule '{name}'.")
                    continue
                
                logger.info(f"Found {num_groups_with_multiple} groups containing vehicles of interest for rule '{name}'. Preparing alerts.")
                
                # --- New alert generation logic ---
                
                all_vehicles_in_groups = groups_with_interest.select(col("component"), explode(col("vehicles")).alias("license_plate"))
                detailed_group_events = all_vehicles_in_groups.join(df_full, "license_plate", "inner")

                event_struct = struct(
                    "car_id", "license_plate", "province", "vehicle_brand", "vehicle_brand_model",
                    "vehicle_color", "vehicle_body_type", "vehicle_brand_year", "camera_name", "camera_id",
                    date_format(to_timestamp("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("event_time"),
                    date_format(col("event_date"), "yyyy-MM-dd").alias("event_date"),
                    "gps_latitude", "gps_longitude",
                    date_format(to_timestamp("created_at"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("created_at")
                )

                component_alerts = detailed_group_events.groupBy("component").agg(
                    collect_list("camera_id").alias("cameras"),
                    collect_list("car_id").alias("car_id_list"),
                    collect_list("province").alias("province_list"),
                    collect_list("vehicle_brand").alias("vehicle_brand_list"),
                    collect_list("vehicle_brand_model").alias("vehicle_brand_model_list"),
                    collect_list("vehicle_color").alias("vehicle_color_list"),
                    collect_list("vehicle_body_type").alias("vehicle_body_type_list"),
                    collect_list("vehicle_brand_year").alias("vehicle_brand_year_list"),
                    collect_list("camera_name").alias("camera_name_list"),
                    collect_list(date_format(to_timestamp("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")).alias("event_time_list"),
                    collect_list(date_format(col("event_date"), "yyyy-MM-dd")).alias("event_date_list"),
                    collect_list("gps_latitude").alias("gps_latitude_list"),
                    collect_list("gps_longitude").alias("gps_longitude_list"),
                    collect_list(date_format(to_timestamp("created_at"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")).alias("created_at_list"),
                    collect_list(event_struct).alias("events_data")
                )
                
                trigger_vehicles = all_vehicles_in_groups.join(df_interest, "license_plate", "inner") \
                                                         .select(col("component"), col("license_plate").alias("triggering_plate"))

                final_alerts = trigger_vehicles.join(component_alerts, "component") \
                    .withColumn("area_name", lit(name)) \
                    .withColumn("area_id", lit(rule_id)) \
                    .withColumn("event_type", lit("area_detection")) \
                    .withColumnRenamed("triggering_plate", "license_plate") \
                    .select(
                        "license_plate", "cameras", "car_id_list", "province_list", "vehicle_brand_list",
                        "vehicle_brand_model_list", "vehicle_color_list", "vehicle_body_type_list",
                        "vehicle_brand_year_list", "camera_name_list", "event_time_list", "event_date_list",
                        "gps_latitude_list", "gps_longitude_list", "created_at_list",
                        "area_name", "area_id", "event_type", "events_data"
                    )

                logger.info(f"Generated {final_alerts.count()} alerts for rule '{name}'.")
                
                if args.kafka_brokers:
                    send_to_kafka(spark, final_alerts, args.kafka_brokers, args.kafka_alerts_topic)

            except Exception as e:
                logger.error(f"Error during graph processing for rule '{name}': {str(e)}", exc_info=True)
            finally:
                # Unpersist cached dataframes for the current rule
                df_full.unpersist()
                df_interest.unpersist()
                events.unpersist()
                edges.unpersist()

    except Exception as e:
        logger.error(f"Fatal error in main execution: {str(e)}", exc_info=True)
    finally:
        if spark:
            logger.info("Stopping SparkSession...")
            spark.stop()
            logger.info("SparkSession stopped.")

if __name__ == "__main__":
    main()