import logging
import uuid
import redis
import json
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, countDistinct, abs, collect_list, size,
    to_json, struct, unix_timestamp,
    min as spark_min, max as spark_max, collect_set,
    current_timestamp, explode, lit, date_format, to_timestamp
)
from graphframes import GraphFrame

# --- การตั้งค่า Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def parse_arguments():
    """แยกวิเคราะห์ Arguments ที่ส่งมาจาก Airflow DAG"""
    parser = argparse.ArgumentParser(description='Vehicle Graph Analysis with Spark')
    
    # พารามิเตอร์สำหรับ PostgreSQL
    parser.add_argument('--postgres-host', type=str, required=True, help='PostgreSQL host')
    parser.add_argument('--postgres-port', type=str, default='5432', help='PostgreSQL port')
    parser.add_argument('--postgres-db', type=str, required=True, help='PostgreSQL database name')
    parser.add_argument('--postgres-user', type=str, required=True, help='PostgreSQL username')
    parser.add_argument('--postgres-password', type=str, required=True, help='PostgreSQL password')
    parser.add_argument('--postgres-table', type=str, default='vehicle_events', help='PostgreSQL table name')
    
    # พารามิเตอร์สำหรับ Kafka
    parser.add_argument('--kafka-brokers', type=str, required=True, help='Kafka brokers')
    parser.add_argument('--kafka-alerts-topic', type=str, default='alerts_topic', help='Kafka alerts topic')
    parser.add_argument('--kafka-log-event-topic', type=str, default='log_event_topic', help='Kafka log event topic (ยอมรับค่าเข้ามาเพื่อความเข้ากันได้ แต่ไม่ได้ใช้งาน)')
    
    # พารามิเตอร์สำหรับการวิเคราะห์
    parser.add_argument('--lookback-hours', type=int, default=24, help='จำนวนชั่วโมงที่มองย้อนหลัง')
    parser.add_argument('--time-threshold-seconds', type=int, default=300, help='กรอบเวลา (วินาที) ที่จะกรอง event ที่น่าสนใจ')
    
    # พารามิเตอร์สำหรับ Redis
    parser.add_argument('--redis-host', type=str, default='redis-primary', help='Redis host')
    parser.add_argument('--redis-port', type=int, default=6379, help='Redis port')
    parser.add_argument('--redis-password', type=str, default='my_password', help='Redis password')
    parser.add_argument('--redis-pattern', type=str, default='petternrecognition:*', help='Pattern ของ key ใน Redis')
    
    return parser.parse_args()

def create_spark_session():
    """สร้าง SparkSession"""
    # (โค้ดส่วนนี้เหมือนเดิม ไม่มีการเปลี่ยนแปลง)
    try:
        active_session = SparkSession.getActiveSession()
        if active_session:
            logger.info("กำลังหยุด SparkSession ที่มีอยู่...")
            active_session.stop()
    except Exception as e:
        logger.warning(f"เกิดข้อผิดพลาดในการหยุด SparkSession: {e}")

    spark_builder = (
        SparkSession.builder
        .appName("Vehicle Graph Analysis (Simple Alert)")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-hadoop-hdfs-nn.speak-test.svc.cluster.local:9000")
        .config("spark.sql.warehouse.dir", "hdfs://hadoop-hadoop-hdfs-nn.speak-test.svc.cluster.local:9000/user/spark/warehouse")
    )
    spark = spark_builder.getOrCreate()
    logger.info("สร้าง SparkSession สำเร็จ")
    checkpoint_base_dir_hdfs = "hdfs://hadoop-hadoop-hdfs-nn.speak-test.svc.cluster.local:9000/spark-checkpoints/graphframes"
    checkpoint_dir_hdfs = f"{checkpoint_base_dir_hdfs}/{uuid.uuid4()}"
    try:
        spark.sparkContext.setCheckpointDir(checkpoint_dir_hdfs)
        logger.info(f"ตั้งค่า Checkpoint directory ของ GraphFrames ไปที่ HDFS: {checkpoint_dir_hdfs}")
    except Exception as e:
        logger.warning(f"ไม่สามารถตั้งค่า HDFS checkpoint directory: {e}. ใช้ local directory แทน")
        local_fallback_checkpoint_dir = f"/tmp/spark-checkpoints/graphframes-fallback/{uuid.uuid4()}"
        spark.sparkContext.setCheckpointDir(local_fallback_checkpoint_dir)
        logger.info(f"ใช้ Local checkpoint directory: {local_fallback_checkpoint_dir}")
    spark.sparkContext.setLogLevel("WARN")
    return spark

def get_rule_data(redis_host, redis_port, redis_password, redis_pattern):
    """ดึงข้อมูล Rule จาก Redis"""
    # (โค้ดส่วนนี้เหมือนเดิม ไม่มีการเปลี่ยนแปลง)
    try:
        redis_client = redis.Redis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)
        rules = []
        rules.append({"rule_id": "default_rule", "name": "default_rule", "number_camera": 3, "time_range": 180, "camera_ids": []})
        logger.info(f"กำลังค้นหา key ใน Redis ด้วย pattern: '{redis_pattern}'")
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
                    logger.warning(f"ข้อมูลสำหรับ key '{key}' ไม่สมบูรณ์ จะข้ามไป")
            except (json.JSONDecodeError, Exception) as e:
                logger.error(f"เกิดข้อผิดพลาดในการประมวลผล key {key}: {e}")
    except redis.exceptions.RedisError as e:
        logger.error(f"เกิดข้อผิดพลาดในการเชื่อมต่อ Redis: {e}")
        return []
    logger.info(f"โหลดข้อมูล Rule สำเร็จ {len(rules)} รายการจาก Redis")
    return rules

def send_to_kafka(spark, df, kafka_brokers, kafka_topic):
    """ส่ง DataFrame ไปยัง Kafka Topic"""
    if df.rdd.isEmpty():
        logger.info(f"DataFrame ว่างเปล่า ไม่มีการส่งข้อมูลไปยัง Kafka topic '{kafka_topic}'")
        return
    try:
        logger.info(f"กำลังพยายามส่งข้อมูลไปยัง Kafka topic '{kafka_topic}'...")
        kafka_output_df = df.select(to_json(struct("*")).alias("value"))
        
        # ไม่จำเป็นต้องตั้งค่า max.request.size อีกต่อไป เพราะ message มีขนาดเล็ก
        (kafka_output_df.write
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_brokers)
            .option("topic", kafka_topic)
            .save())
        logger.info(f"ส่งข้อมูลไปยัง Kafka topic '{kafka_topic}' สำเร็จ")
    except Exception as kafka_e:
        logger.error(f"เกิดข้อผิดพลาดในการส่งข้อมูลไป Kafka: {str(kafka_e)}", exc_info=True)
        raise kafka_e

def main():
    """ฟังก์ชันหลักในการทำงาน"""
    args = parse_arguments()
    logger.info(f"ได้รับพารามิเตอร์: PG={args.postgres_host}, Kafka={args.kafka_brokers}, Lookback={args.lookback_hours}h")
    
    spark = None
    try:
        spark = create_spark_session()
        jdbc_url = f"jdbc:postgresql://{args.postgres_host}:{args.postgres_port}/{args.postgres_db}"
        list_data = get_rule_data(args.redis_host, args.redis_port, args.redis_password, args.redis_pattern)
        
        for rule in list_data:
            # --- ส่วนของการโหลดข้อมูลและหา df_interest ยังคงเหมือนเดิม ---
            rule_id, name, number_camera, time_range, camera_ids = rule.values()
            
            camera_filter = ""
            if camera_ids:
                camera_ids_sql_format = ", ".join([f"'{cam_id}'" for cam_id in camera_ids])
                camera_filter = f" AND camera_id IN ({camera_ids_sql_format})"

            # เรายังคงต้องดึง camera_id และ event_time เพื่อใช้ในการคำนวณ
            dbtable_query = (
                f"(SELECT license_plate, camera_id, camera_name, event_time "
                f" FROM {args.postgres_table} "
                f" WHERE event_time >= NOW() - INTERVAL '{args.lookback_hours} hours'{camera_filter}"
                f") AS filtered_data"
            )
            logger.info(f"กำลัง Query ข้อมูลสำหรับ rule '{name}' ย้อนหลัง {args.lookback_hours} ชั่วโมง")

            try:
                df_full = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", dbtable_query).option("user", args.postgres_user).option("password", args.postgres_password).option("driver", "org.postgresql.Driver").option("fetchsize", "10000").load()
                df_full.cache()
                if df_full.rdd.isEmpty():
                    logger.warning(f"ไม่พบข้อมูลสำหรับ rule '{name}'. Skipping.")
                    continue
                logger.info(f"โหลดข้อมูล {df_full.count()} records สำหรับ rule '{name}' สำเร็จ")

                df = df_full.select("license_plate", "camera_name", "event_time").withColumn("timestamp_utc", unix_timestamp(col("event_time")))
                
                current_time_unix = unix_timestamp(current_timestamp())
                df_interest = df.groupBy("license_plate").agg(spark_max("timestamp_utc").alias("latest_event_unix")).filter((current_time_unix - col("latest_event_unix")) <= args.time_threshold_seconds).select("license_plate").cache()
                
                if df_interest.rdd.isEmpty():
                    logger.info(f"ไม่พบยานพาหนะที่น่าสนใจสำหรับ rule '{name}'. Skipping.")
                    continue
                logger.info(f"พบ {df_interest.count()} ป้ายทะเบียนที่น่าสนใจสำหรับ rule '{name}'")
            except Exception as e:
                logger.error(f"ไม่สามารถโหลดข้อมูลจาก PostgreSQL สำหรับ rule '{name}': {str(e)}")
                continue

            try:
                # --- ส่วนของการสร้าง Graph ยังคงเหมือนเดิม ---
                events = df.withColumnRenamed("license_plate", "vehicle").withColumnRenamed("camera_name", "point").withColumnRenamed("timestamp_utc", "timestamp")
                events.cache()
                e1, e2 = events.alias("a"), events.alias("b")
                raw_edges = e1.join(e2, (col("a.point") == col("b.point")) & (col("a.vehicle") < col("b.vehicle")) & (abs(col("a.timestamp") - col("b.timestamp")) <= time_range)).select(col("a.vehicle").alias("src"), col("b.vehicle").alias("dst"), col("a.point"))
                edges = raw_edges.groupBy("src", "dst").agg(countDistinct("point").alias("common_points")).filter(col("common_points") >= number_camera).cache()
                if edges.rdd.isEmpty():
                    logger.warning(f"ไม่พบ Edge ที่ตรงตามเงื่อนไขสำหรับ rule '{name}'.")
                    continue
                verts = events.select(col("vehicle").alias("id")).distinct()
                g = GraphFrame(verts, edges.select("src", "dst"))
                cc = g.connectedComponents()
                groups_all = cc.groupBy("component").agg(collect_list("id").alias("vehicles")).filter(size(col("vehicles")) > 1)
                groups_exploded = groups_all.select(col("component"), explode(col("vehicles")).alias("vehicle"))
                groups_with_interest = groups_exploded.join(df_interest.withColumnRenamed("license_plate", "vehicle"), "vehicle", "inner").select("component").distinct().join(groups_all, "component")
                if groups_with_interest.rdd.isEmpty():
                    logger.info(f"ไม่พบกลุ่มที่มีรถที่น่าสนใจสำหรับ rule '{name}'.")
                    continue
                logger.info(f"พบ {groups_with_interest.count()} กลุ่มที่น่าสนใจสำหรับ rule '{name}'. กำลังเตรียมสร้าง Alerts")
                
                # =================================================================
                # ===== แก้ไขส่วนการสร้าง ALERT ให้เป็นรูปแบบใหม่ง่ายๆ ตรงนี้ =====
                # =================================================================

                # 1. นำรายชื่อรถทั้งหมดในกลุ่มที่น่าสนใจ มา join กับข้อมูลดิบ (df_full)
                all_vehicles_in_groups = groups_with_interest.select(col("component"), explode(col("vehicles")).alias("license_plate"))
                detailed_group_events = all_vehicles_in_groups.join(df_full, "license_plate", "inner")

                # 2. รวบรวมข้อมูลของแต่ละกลุ่มเพื่อหา เวลาเริ่มต้น, เวลาสิ้นสุด, และรายชื่อกล้อง
                aggregated_group_summary = detailed_group_events.groupBy("component").agg(
                    spark_min("event_time").alias("start_time"),
                    spark_max("event_time").alias("end_time"),
                    collect_set("camera_id").alias("cameras") # ใช้ collect_set เพื่อเอากล้องที่ไม่ซ้ำ
                )

                # 3. หารถที่เป็นตัวจุดชนวน (trigger) ของแต่ละกลุ่ม
                trigger_vehicles = all_vehicles_in_groups.join(df_interest, "license_plate", "inner") \
                                                         .select(col("component"), col("license_plate"))

                # 4. นำข้อมูลที่รวบรวมไว้ของกลุ่ม มา join กับรถที่เป็นตัวจุดชนวน
                final_alerts = trigger_vehicles.join(aggregated_group_summary, "component")

                # 5. จัดรูปแบบเวลาและเลือกคอลัมน์สุดท้ายสำหรับส่งไป Kafka
                final_alerts = final_alerts.select(
                    col("license_plate"),
                    date_format(col("start_time"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("start_time"),
                    date_format(col("end_time"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("end_time"),
                    col("cameras")
                )
                
                logger.info(f"สร้าง Alerts รูปแบบใหม่ทั้งหมด {final_alerts.count()} รายการสำหรับ rule '{name}'")
                
                if args.kafka_brokers:
                    send_to_kafka(spark, final_alerts, args.kafka_brokers, args.kafka_alerts_topic)

            except Exception as e:
                logger.error(f"เกิดข้อผิดพลาดระหว่างประมวลผล Graph สำหรับ rule '{name}': {str(e)}", exc_info=True)
            finally:
                df_full.unpersist()
                df_interest.unpersist()
                events.unpersist()
                edges.unpersist()

    except Exception as e:
        logger.error(f"เกิดข้อผิดพลาดร้ายแรงในการทำงานหลัก: {str(e)}", exc_info=True)
    finally:
        if spark:
            logger.info("กำลังหยุด SparkSession...")
            spark.stop()
            logger.info("หยุด SparkSession สำเร็จ")

if __name__ == "__main__":
    main()