import logging
import argparse
import requests # ต้องติดตั้งไลบรารีก่อน: pip install requests
import redis    # ต้องติดตั้งไลบรารีก่อน: pip install redis
from uuid import uuid4, uuid5 # เพิ่มการ import uuid
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, StringType, StructType, StructField

def parse_arguments():
    """แยกวิเคราะห์ Arguments ที่ส่งมาจาก Airflow DAG หรือ Command Line"""
    parser = argparse.ArgumentParser(description="Vehicle Speed Analysis with Kafka Alerts using OSRM and Redis Cache")
    
    # พารามิเตอร์สำหรับ PostgreSQL
    parser.add_argument('--postgres-host', type=str, required=True, help='PostgreSQL host')
    parser.add_argument('--postgres-port', type=int, default=5432, help='PostgreSQL port')
    parser.add_argument('--postgres-db', type=str, required=True, help='PostgreSQL database name')
    parser.add_argument('--postgres-user', type=str, required=True, help='PostgreSQL username')
    parser.add_argument('--postgres-password', type=str, required=True, help='PostgreSQL password')
    parser.add_argument('--postgres-table', type=str, default='vehicle_events', help='PostgreSQL table name')
    
    # พารามิเตอร์สำหรับ Kafka
    parser.add_argument('--kafka-brokers', type=str, required=True, help='Kafka bootstrap servers (e.g., "host1:9092,host2:9092")')
    parser.add_argument('--kafka-alerts-topic', type=str, default='alerts_topic', help='Kafka topic for high-speed alerts')
    parser.add_argument('--kafka-log-event-topic', type=str, default='log_event_topic', help='Kafka topic for event logs that cause alerts')
    
    # พารามิเตอร์สำหรับการวิเคราะห์
    parser.add_argument('--lookback-hours', type=int, default=24, help='จำนวนชั่วโมงที่มองย้อนหลังสำหรับข้อมูล')
    
    # พารามิเตอร์สำหรับ OSRM
    parser.add_argument('--osrm-server-url', type=str, default="http://router.project-osrm.org", help='URL of the OSRM backend server')

    # พารามิเตอร์สำหรับ Redis
    parser.add_argument('--redis-host', type=str, default='localhost', help='Redis host')
    parser.add_argument('--redis-port', type=int, default=6379, help='Redis port')
    parser.add_argument('--redis-db', type=int, default=0, help='Redis database number')
    parser.add_argument('--redis-password', type=str, help='Redis password (if required)')

    return parser.parse_args()

def write_to_kafka(df, topic, kafka_bootstrap_servers, logger):
    """Writes a DataFrame with a 'value' column to a Kafka topic."""
    if df.rdd.isEmpty():
        logger.info(f"No records to write to Kafka topic: {topic}")
        return

    logger.info(f"Writing {df.count()} records to Kafka topic: {topic}")
    (df
        .select(F.col("value").cast(StringType()))
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("topic", topic)
        .save()
    )

def main():
    """Main execution function."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    args = parse_arguments()

    spark = (
        SparkSession.builder
        .appName("VehicleSpeedAnalysisWithKafkaOSRM")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark Session created. Running with arguments: {vars(args)}")
    
    # --- สร้าง Namespace สำหรับ UUID และ Broadcast ไปยัง Worker Nodes ---
    set_uuid = uuid4()
    namespace_broadcast = spark.sparkContext.broadcast(set_uuid)

    # --- Load Data from PostgreSQL ---
    dbtable_query = (
        f"(SELECT * FROM {args.postgres_table} "
        f"WHERE event_time >= NOW() - INTERVAL '{args.lookback_hours} hours') AS filtered_data"
    )
    logger.info(f"Querying last {args.lookback_hours} hours from table {args.postgres_table}.")
    
    jdbc_url = f"jdbc:postgresql://{args.postgres_host}:{args.postgres_port}/{args.postgres_db}"

    try:
        df = spark.read.format("jdbc").options(
            url=jdbc_url,
            dbtable=dbtable_query,
            user=args.postgres_user,
            password=args.postgres_password,
            driver="org.postgresql.Driver",
            fetchsize="10000"
        ).load().cache()
        logger.info(f"Loaded {df.count()} records from PostgreSQL.")
        if df.rdd.isEmpty():
            logger.warning("No data loaded. Stopping execution.")
            spark.stop()
            return
    except Exception as e:
        logger.error(f"Failed to load data from PostgreSQL: {e}", exc_info=True)
        spark.stop()
        return

    # --- Prepare Previous Log Data ---
    all_columns = df.columns
    vehicle_window = Window.partitionBy("car_id").orderBy("event_time")

    df_with_prev_log = df
    for col_name in all_columns:
        df_with_prev_log = df_with_prev_log.withColumn(f"prev_{col_name}", F.lag(col_name).over(vehicle_window))
    
    df_to_process = df_with_prev_log.filter(F.col("prev_car_id").isNotNull()).cache()

    # --- Define the processing function for mapPartitions ---
    def process_partition(iterator):
        redis_client = redis.Redis(
            host=args.redis_host, 
            port=args.redis_port, 
            db=args.redis_db, 
            password=args.redis_password, 
            decode_responses=True
        )
        
        for row in iterator:
            row_dict = row.asDict()
            
            lat1, lon1 = row_dict.get('prev_gps_latitude'), row_dict.get('prev_gps_longitude')
            lat2, lon2 = row_dict.get('gps_latitude'), row_dict.get('gps_longitude')
            cam1, cam2 = row_dict.get('prev_camera_name'), row_dict.get('camera_name')

            distance_km = None

            if all([lat1, lon1, lat2, lon2, cam1, cam2]):
                cache_key = f"osrm_dist:{':'.join(sorted([cam1, cam2]))}"
                
                try:
                    cached_distance = redis_client.get(cache_key)
                    if cached_distance is not None:
                        distance_km = float(cached_distance)
                    else:
                        request_url = f"{args.osrm_server_url}/route/v1/driving/{lon1},{lat1};{lon2},{lat2}?overview=false"
                        response = requests.get(request_url, timeout=5)
                        response.raise_for_status()
                        data = response.json()
                        if data.get('code') == 'Ok' and data.get('routes'):
                            distance_meters = data['routes'][0]['distance']
                            distance_km = distance_meters / 1000.0
                            redis_client.set(cache_key, distance_km, ex=86400)
                except Exception:
                    pass
            
            row_dict['distance_km'] = distance_km
            
            time_delta_seconds = (row_dict['event_time'] - row_dict['prev_event_time']).total_seconds()
            time_delta_hours = time_delta_seconds / 3600.0 if time_delta_seconds > 0 else 0
            speed_kmh = round(distance_km / time_delta_hours, 2) if time_delta_hours > 0 and distance_km is not None else 0.0

            row_dict['time_delta_seconds'] = time_delta_seconds
            row_dict['time_delta_hours'] = time_delta_hours
            row_dict['speed_kmh'] = speed_kmh
            
            yield row_dict

    output_schema = StructType(df_to_process.schema.fields + [
        StructField("distance_km", FloatType(), True),
        StructField("time_delta_seconds", FloatType(), True),
        StructField("time_delta_hours", FloatType(), True),
        StructField("speed_kmh", FloatType(), True)
    ])

    results_rdd = df_to_process.rdd.mapPartitions(process_partition)
    results_df = spark.createDataFrame(results_rdd, schema=output_schema)

    # --- สร้าง UUID สำหรับแต่ละ record ---
    uuid_udf = F.udf(
        lambda plate: str(uuid5(namespace_broadcast.value, plate)) if plate else None,
        StringType()
    )
    results_df_with_uuid = results_df.withColumn("record_uuid", uuid_udf(F.col("license_plate"))) \
                                     .withColumn("prev_record_uuid", uuid_udf(F.col("prev_license_plate")))


    # --- Filter for High-Speed Events ---
    high_speed_df = results_df_with_uuid.filter(F.col("speed_kmh") > 250).cache()
    logger.info(f"Found {high_speed_df.count()} high-speed events (speed > 250 km/h).")

    if not high_speed_df.rdd.isEmpty():
        # --- Prepare and Send to Kafka Topics ---

        # ✅ 1. สร้าง DataFrame สำหรับ Log Event (แยกแต่ละ Event เป็นคนละ Record)

        # เลือกคอลัมน์สำหรับ "Event ปัจจุบัน"
        df_current_event = high_speed_df.select(
            F.col("record_uuid"),
            F.col("car_id"),
            F.col("license_plate"),
            F.col("province"),
            F.col("vehicle_brand"),
            F.col("vehicle_color"),
            F.col("camera_name"),
            F.col("camera_id"),
            F.col("event_time"),
            F.col("event_date"),
            F.col("gps_latitude"),
            F.col("gps_longitude"),
            F.col("created_at"),
            F.col("distance_km"),
            F.col("time_delta_seconds"),
            F.col("speed_kmh"),
            F.lit("ghost_detection").alias("event_type")
        )

        # เลือกคอลัมน์สำหรับ "Event ก่อนหน้า" และเปลี่ยนชื่อคอลัมน์ให้ตรงกัน
        df_prev_event = high_speed_df.select(
            F.col("prev_record_uuid").alias("record_uuid"),
            F.col("prev_car_id").alias("car_id"),
            F.col("prev_license_plate").alias("license_plate"),
            F.col("prev_province").alias("province"),
            F.col("prev_vehicle_brand").alias("vehicle_brand"),
            F.col("prev_vehicle_color").alias("vehicle_color"),
            F.col("prev_camera_name").alias("camera_name"),
            F.col("prev_camera_id").alias("camera_id"),
            F.col("prev_event_time").alias("event_time"),
            F.col("prev_event_date").alias("event_date"),
            F.col("prev_gps_latitude").alias("gps_latitude"),
            F.col("prev_gps_longitude").alias("gps_longitude"),
            F.col("prev_created_at").alias("created_at"),
            # --- ส่วนที่แก้ไข ---
            # เพิ่มคอลัมน์ที่ขาดไปโดยกำหนดค่าเป็น NULL เพื่อให้ Schema ตรงกัน
            F.lit(None).cast(FloatType()).alias("distance_km"),
            F.lit(None).cast(FloatType()).alias("time_delta_seconds"),
            F.lit(None).cast(FloatType()).alias("speed_kmh"),
            # --------------------
            F.lit("ghost_detection").alias("event_type")
        )

        # รวมทั้งสอง DataFrame เข้าด้วยกัน ให้แต่ละ event เป็นคนละแถว
        unioned_df = df_current_event.unionByName(df_prev_event)

        # สร้าง JSON จาก struct ของแต่ละแถว (ไม่ใช่ array)
        log_event_df = unioned_df.select(
            F.to_json(
                F.struct(
                    "record_uuid", "car_id", "license_plate", "province", "vehicle_brand",
                    "vehicle_color", "camera_name", "camera_id", "event_time", "event_date",
                    "gps_latitude", "gps_longitude", "created_at", "event_type","distance_km","time_delta_seconds","speed_kmh"
                )
            ).alias("value")
        )

        # ส่งข้อมูล log ทีละ event ไปยัง Kafka
        write_to_kafka(log_event_df, args.kafka_log_event_topic, args.kafka_brokers, logger)


        # ✅ 2. สร้าง DataFrame สำหรับ Alert (ยังคงรวมข้อมูลเป็นชุดเดียว)
        alert_event_df = high_speed_df.select(
            F.to_json(
                F.struct(
                    F.col("license_plate"),
                    F.array("prev_record_uuid", "record_uuid").alias("record_uuid_list"),
                    F.array("prev_camera_id", "camera_id").alias("cameras"),
                    F.array("prev_car_id", "car_id").alias("car_id_list"),
                    F.array("prev_province", "province").alias("province_list"),
                    F.array("prev_vehicle_brand", "vehicle_brand").alias("vehicle_brand_list"),
                    F.array("prev_vehicle_brand_model", "vehicle_brand_model").alias("vehicle_brand_model_list"),
                    F.array("prev_vehicle_color", "vehicle_color").alias("vehicle_color_list"),
                    F.array("prev_vehicle_body_type", "vehicle_body_type").alias("vehicle_body_type_list"),
                    F.array("prev_vehicle_brand_year", "vehicle_brand_year").alias("vehicle_brand_year_list"),
                    F.array("prev_camera_name", "camera_name").alias("camera_name_list"),
                    F.array("prev_event_time", "event_time").alias("event_time_list"),
                    F.array("prev_event_date", "event_date").alias("event_date_list"),
                    F.array("prev_gps_latitude", "gps_latitude").alias("gps_latitude_list"),
                    F.array("prev_gps_longitude", "gps_longitude").alias("gps_longitude_list"),
                    F.array("prev_created_at", "created_at").alias("created_at_list"),
                    F.lit("ghost_detection").alias("event_type")
                )
            ).alias("value")
        )
        write_to_kafka(alert_event_df, args.kafka_alerts_topic, args.kafka_brokers, logger)
    
    spark.stop()
    logger.info("Spark Session stopped.")

if __name__ == "__main__":
    main()
