import logging
import uuid
import redis
import json
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, countDistinct, abs, collect_list, size,
    to_json, struct, unix_timestamp, max as spark_max,
    current_timestamp
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
    parser.add_argument('--kafka-log-event-topic', type=str, default='log_event_topic', help='Kafka log event topic')
    
    # Analysis parameters
    parser.add_argument('--lookback-hours', type=int, default=24, help='Hours to look back for data')
    parser.add_argument('--time-threshold-seconds', type=int, default=300, help='Time threshold in seconds')
    
    # Redis parameters (with defaults)
    parser.add_argument('--redis-host', type=str, default='redis-primary', help='Redis host')
    parser.add_argument('--redis-port', type=int, default=6379, help='Redis port')
    parser.add_argument('--redis-password', type=str, default='my_password', help='Redis password')
    parser.add_argument('--redis-pattern', type=str, default='petternrecognition:*', help='Redis pattern for keys')
    
    return parser.parse_args()

def create_spark_session():
    """Create SparkSession with HDFS configurations for distributed environment"""
    try:
        # Stop existing SparkSession if any (useful for interactive sessions)
        active_session = SparkSession.getActiveSession()
        if active_session:
            logger.info("Stopping existing SparkSession...")
            active_session.stop()
            logger.info("Stopped existing SparkSession.")
    except Exception as e:
        logger.warning(f"Error stopping existing SparkSession: {e}")
        pass # Continue if no active session or error stopping

    spark_builder = (
        SparkSession.builder
        .appName("Vehicle Graph Analysis (Distributed with HDFS)")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        # HDFS configurations
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-hadoop-hdfs-nn.speak-test.svc.cluster.local:9000")
        .config("spark.sql.warehouse.dir", "hdfs://hadoop-hadoop-hdfs-nn.speak-test.svc.cluster.local:9000/user/spark/warehouse")
    )

    spark = spark_builder.getOrCreate()
    logger.info("SparkSession created.")

    # --- HDFS Checkpoint Directory for GraphFrames ---
    checkpoint_base_dir_hdfs = "hdfs://hadoop-hadoop-hdfs-nn.speak-test.svc.cluster.local:9000/spark-checkpoints/graphframes"
    checkpoint_dir_hdfs = f"{checkpoint_base_dir_hdfs}/{uuid.uuid4()}"

    try:
        # Ensure HDFS path '/spark-checkpoints/graphframes' exists and is writable.
        spark.sparkContext.setCheckpointDir(checkpoint_dir_hdfs)
        logger.info(f"GraphFrames checkpoint directory set to HDFS: {checkpoint_dir_hdfs}")
    except Exception as e:
        logger.warning(f"Failed to set HDFS checkpoint directory: {e}")
        local_fallback_checkpoint_dir = f"/tmp/spark-checkpoints/graphframes-fallback/{uuid.uuid4()}"
        try:
            spark.sparkContext.setCheckpointDir(local_fallback_checkpoint_dir)
            logger.info(f"Falling back to local checkpoint directory: {local_fallback_checkpoint_dir}")
            logger.warning("Using local checkpointing may impact GraphFrames performance and reliability in a distributed cluster.")
        except Exception as local_e:
            logger.error(f"Failed to set even local fallback checkpoint directory: {local_e}")
            logger.warning("Continuing without a reliable checkpoint directory. GraphFrames might fail on complex operations.")

    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession configured with HDFS and checkpoint directory.")
    return spark

def get_rule_data(redis_host, redis_port, redis_password, redis_pattern):
    """Get rule data from Redis with configurable parameters"""
    try:
        redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            decode_responses=True
        )
        
        rule = []
        # Add default rule
        rule.append({
            "rule_id": "default_rule",
            "name": "default_rule",
            "number_camera": 3,
            "time_range": 180,
            "camera_ids": []
        })
        
        logger.info(f"Scanning Redis for keys matching pattern: '{redis_pattern}'")
        for key in redis_client.scan_iter(match=redis_pattern, count=1000):
            try:
                payload_str = redis_client.get(key)
                if not payload_str:
                    logger.warning(f"Key '{key}' has no payload. Skipping.")
                    continue
                payload = json.loads(payload_str)
                id = payload.get("id")
                name = payload.get("name")
                number_camera = payload.get("number_camera")
                time_range = payload.get("time_range")
                camera_ids = payload.get("camera_id", [])

                if id and name and number_camera and time_range and camera_ids:
                    rule.append({
                        "rule_id": id,
                        "name": name,
                        "number_camera": number_camera,
                        "time_range": time_range,
                        "camera_ids": camera_ids
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
        
    logger.info(f"Successfully loaded {len(rule)} pattern configurations from Redis.")
    return rule

def send_to_kafka(spark, df, kafka_brokers, kafka_topic):
    """Send DataFrame to Kafka topic"""
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
    # Parse command line arguments
    args = parse_arguments()
    
    # Log the received parameters
    logger.info("Received parameters:")
    logger.info(f"  PostgreSQL: {args.postgres_host}:{args.postgres_port}/{args.postgres_db}")
    logger.info(f"  Kafka: {args.kafka_brokers}")
    logger.info(f"  Lookback hours: {args.lookback_hours}")
    logger.info(f"  Time threshold: {args.time_threshold_seconds} seconds")
    logger.info(f"  Redis: {args.redis_host}:{args.redis_port}")
    
    spark = None
    try:
        spark = create_spark_session()
        logger.info("SparkSession created successfully for main execution.")

        jdbc_url = f"jdbc:postgresql://{args.postgres_host}:{args.postgres_port}/{args.postgres_db}"

        # Get rule data from Redis
        list_data = get_rule_data(args.redis_host, args.redis_port, args.redis_password, args.redis_pattern)
        
        for i in list_data:
            rule_id = i["rule_id"]
            name = i["name"]
            number_camera = i["number_camera"]
            time_range = i["time_range"]
            camera_ids = i["camera_ids"]
            
            # Handle empty camera_ids for default rule
            if not camera_ids:
                logger.info(f"No specific camera IDs for rule '{name}', processing all cameras.")
                camera_filter = ""
            else:
                camera_ids_sql_format = ", ".join([f"'{cam_id}'" for cam_id in camera_ids])
                camera_filter = f" AND camera_id IN ({camera_ids_sql_format})"

            # Prepare SQL query
            if args.lookback_hours:
                dbtable_query = (
                    f"(SELECT license_plate, camera_name, event_time "
                    f" FROM {args.postgres_table} "
                    f" WHERE event_time >= NOW() - INTERVAL '{args.lookback_hours} hours'{camera_filter}"
                    f") AS filtered_data"
                )
                logger.info(f"Loading last {args.lookback_hours}h of data for rule '{name}'.")
            else:
                dbtable_query = (
                    f"(SELECT license_plate, camera_name, event_time "
                    f" FROM {args.postgres_table} "
                    f" WHERE 1=1{camera_filter}) AS all_data"
                )
                logger.info(f"Loading all data for rule '{name}'.")

            # Load data from PostgreSQL
            try:
                df = (
                    spark.read.format("jdbc")
                    .option("url", jdbc_url)
                    .option("dbtable", dbtable_query)
                    .option("user", args.postgres_user)
                    .option("password", args.postgres_password)
                    .option("driver", "org.postgresql.Driver")
                    .option("fetchsize", "1000")
                    .option("numPartitions", "4")
                    .load()
                )

                df = df.withColumn("timestamp_utc", unix_timestamp(col("event_time"))).drop("event_time")
                
                # Cache the dataframe
                df.cache()
                current_time_unix = unix_timestamp(current_timestamp())
                df_interest = (
                    df.groupBy("license_plate")
                    .agg(spark_max("timestamp_utc").alias("latest_event_unix"))
                    .filter((current_time_unix - col("latest_event_unix")) <= args.time_threshold_seconds)
                    .select("license_plate")
                )
                logger.info(f"Found {df_interest.count()} license plates with recent activity (within {args.time_threshold_seconds} seconds)")
                
                record_count = df.count()
                logger.info(f"Loaded {record_count} records successfully.")

                if record_count == 0:
                    logger.warning("No data found! Check your database connection, table, and lookback hours filter.")
                    continue  # Continue with next rule instead of returning

                # df.printSchema()
                df.show(5, truncate=False)

            except Exception as e:
                logger.error(f"Failed to load data from PostgreSQL: {str(e)}")
                continue  # Continue with next rule instead of raising

            # Process graph analysis
            try:
                # Rename columns for clarity
                events = (
                    df.withColumnRenamed("license_plate", "vehicle")
                    .withColumnRenamed("camera_name", "point")
                    .withColumnRenamed("timestamp_utc", "timestamp")
                )

                # Cache events
                events.cache()
                logger.info(f"Processing {events.count()} events for graph analysis.")

                # Build raw edges by co‑occurrence in same point within time threshold
                e1 = events.alias("a")
                e2 = events.alias("b")

                raw_edges = (
                    e1.join(e2,
                            (col("a.point") == col("b.point")) &
                            (col("a.vehicle") < col("b.vehicle")) &
                            (abs(col("a.timestamp") - col("b.timestamp")) <= time_range)
                    )
                    .select(
                        col("a.vehicle").alias("src"),
                        col("b.vehicle").alias("dst"),
                        col("a.point").alias("point")
                    )
                )
                logger.info("Raw edges computed.")

                # Filter edges to those with enough common points
                edges = (
                    raw_edges.groupBy("src", "dst")
                            .agg(countDistinct("point").alias("common_points"))
                            .filter(col("common_points") >= number_camera)
                )

                # Cache edges
                edges.cache()
                edge_count = edges.count()
                logger.info(f"Found {edge_count} edges with >= {number_camera} common points.")

                # Create vertices
                verts = events.select(col("vehicle").alias("id")).distinct()
                vert_count = verts.count()
                logger.info(f"Found {vert_count} unique vehicles (vertices).")

                if vert_count == 0:
                    logger.warning("No vehicles found to create a graph.")
                    continue

                g = None
                if edge_count == 0:
                    logger.warning("No edges met the criteria. Graph will only contain isolated vertices.")
                    empty_edges_schema = spark.createDataFrame([], raw_edges.select("src", "dst").schema)
                    g = GraphFrame(verts, empty_edges_schema)
                else:
                    g = GraphFrame(verts, edges.select("src", "dst"))
                    logger.info("GraphFrame created successfully with vertices and edges.")

                # Compute connected components
                logger.info("Computing connected components...")
                try:
                    cc = g.connectedComponents()
                    logger.info("Connected components computed successfully.")
                except Exception as graph_error:
                    logger.error(f"GraphFrame connectedComponents failed: {graph_error}")
                    logger.info("Falling back to assigning each vertex to its own component.")
                    cc = verts.withColumn("component", col("id"))

                logger.info("Connected components result:")
                cc.show(20, truncate=False)
                
                # Original groups (before filtering)
                groups_all = (
                    cc.groupBy("component")
                    .agg(collect_list("id").alias("vehicles"))
                    .filter(size(col("vehicles")) > 1)
                )

                num_groups_all = groups_all.count()
                logger.info(f"Total number of groups (connected components) with more than one vehicle: {num_groups_all}")

                # Filter groups to include only those with at least one vehicle from df_interest
                df_interest_vehicles = df_interest.withColumnRenamed("license_plate", "vehicle_of_interest")
                
                # Join groups with df_interest to find groups containing vehicles of interest
                from pyspark.sql.functions import array_contains, explode
                
                # Explode the vehicles array to individual rows for easier joining
                groups_exploded = groups_all.select(
                    col("component"),
                    col("vehicles"),
                    explode(col("vehicles")).alias("vehicle")
                )
                
                # Join with df_interest to find which groups contain vehicles of interest
                groups_with_interest = (
                    groups_exploded
                    .join(df_interest.withColumnRenamed("license_plate", "vehicle"), "vehicle", "inner")
                    .select("component", "vehicles")
                    .distinct()
                )
                
                # Final filtered groups
                groups = groups_with_interest
                
                num_groups_with_multiple = groups.count()
                logger.info(f"Number of groups with multiple vehicles AND containing at least one vehicle of interest: {num_groups_with_multiple}")

                if num_groups_with_multiple > 0:
                    logger.info(f"Groups with multiple vehicles that contain vehicles of interest (top 20):")
                    groups.show(20, truncate=False)
                    
                    # Optional: Show which vehicles in each group are of interest
                    logger.info("Detailed breakdown of groups showing vehicles of interest:")
                    groups_detail = (
                        groups_exploded
                        # 1. Join โดยระบุเงื่อนไขให้ชัดเจน และเปลี่ยนชื่อคอลัมน์จาก df_interest เพื่อไม่ให้ซ้ำซ้อน
                        .join(df_interest.withColumnRenamed("license_plate", "vehicle_check"),
                            col("vehicle") == col("vehicle_check"),
                            "left")
                        # 2. ตรวจสอบ isNotNull จากคอลัมน์ที่ join เข้ามาใหม่
                        .withColumn("is_of_interest", col("vehicle_check").isNotNull())
                        .groupBy("component", "vehicles")
                        .agg(
                            collect_list(
                                struct(
                                    col("vehicle").alias("vehicle"),
                                    col("is_of_interest").alias("is_of_interest")
                                )
                            ).alias("vehicle_details")
                        )
                        .join(groups_with_interest.select("component"), "component", "inner")
                    )
                    groups_detail.show(20, truncate=False)
                    
                    # Send results to Kafka
                    if args.kafka_brokers:
                        # Send to alerts topic
                        send_to_kafka(spark, groups, args.kafka_brokers, args.kafka_alerts_topic)
                        
                        # Optionally send detailed results to log event topic
                        send_to_kafka(spark, groups_detail, args.kafka_brokers, args.kafka_log_event_topic)
                    
                else:
                    logger.info("No groups found with more than one vehicle that contain vehicles of interest.")

                logger.info(f"Graph analysis completed successfully for rule '{name}'.")

            except Exception as e:
                logger.error(f"Error during graph processing for rule '{name}': {str(e)}", exc_info=True)
                # Fallback if graph processing has a major error
                logger.info("Performing simple fallback: list distinct vehicles if graph analysis failed.")
                try:
                    distinct_vehicles = df.select("license_plate").distinct()
                    logger.info(f"Distinct vehicles found (fallback): {distinct_vehicles.count()}")
                    distinct_vehicles.show(20, truncate=False)
                except Exception as fallback_e:
                    logger.error(f"Error in fallback distinct vehicle listing: {fallback_e}")

    except Exception as e:
        logger.error(f"Fatal error in main execution: {str(e)}", exc_info=True)
    finally:
        if spark:
            try:
                logger.info("Stopping SparkSession in finally block...")
                spark.stop()
                logger.info("SparkSession stopped successfully.")
            except Exception as e:
                logger.error(f"Error stopping SparkSession in finally block: {e}")

if __name__ == "__main__":
    main()