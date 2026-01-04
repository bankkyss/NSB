import logging
import uuid
import redis
import json
import argparse
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, countDistinct, abs, collect_list, size,
    to_json, struct, unix_timestamp, expr,
    min as spark_min, max as spark_max, collect_set,
    current_timestamp, explode, lit, date_format, to_timestamp,
    hour, when, sum as spark_sum, broadcast
)
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window
from datetime import timedelta

from graphframes import GraphFrame

# --- ฟังก์ชันอื่นๆ เหมือนเดิม ---
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
    parser.add_argument("--parquet-cache-path", default="", help="Parquet cache path (e.g., rustfs mount). When set, only incremental data is loaded from PostgreSQL")
    parser.add_argument("--redis-checkpoint-key", default="vehicle_graph_analysis:checkpoint_ts", help="Redis key for the last processed event_time checkpoint")
    parser.add_argument("--checkpoint-overlap-seconds", type=int, default=60, help="Overlap window in seconds when querying from the checkpoint to avoid late events")
    
    return parser.parse_args()

def create_spark_session():
    """สร้าง SparkSession"""
    try:
        active_session = SparkSession.getActiveSession()
        if active_session:
            logger.info("กำลังหยุด SparkSession ที่มีอยู่...")
            active_session.stop()
    except Exception as e:
        logger.warning(f"เกิดข้อผิดพลาดในการหยุด SparkSession: {e}")

    spark_builder = (
        SparkSession.builder
        .appName("Vehicle Group Alert v2")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", "100m")
        .config("spark.sql.broadcastTimeout", "600")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        # .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-hadoop-hdfs-nn.speak-test.svc.cluster.local:9000") # Removed to prevent S3A staging error
        # .config("spark.sql.warehouse.dir", "hdfs://hadoop-hadoop-hdfs-nn.speak-test.svc.cluster.local:9000/user/spark/warehouse") # Removed to prevent S3A staging error
        .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
        .config("spark.hadoop.fs.s3a.change.detection.version.required", "false")
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
        # local_fallback_checkpoint_dir = f"file:///tmp/spark-checkpoints/graphframes-fallback/{uuid.uuid4()}"
        local_fallback_checkpoint_dir = f"/tmp/spark-checkpoints/graphframes-fallback/{uuid.uuid4()}"
        spark.sparkContext.setCheckpointDir(local_fallback_checkpoint_dir)
        logger.info(f"ใช้ Local checkpoint directory: {local_fallback_checkpoint_dir}")
    spark.sparkContext.setLogLevel("WARN")
    return spark

def get_rule_data(redis_host, redis_port, redis_password, redis_pattern):
    """ดึงข้อมูล Rule จาก Redis"""
    try:
        redis_client = redis.Redis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)
        rules = []
        # rules.append({"rule_id": "default_rule", "name": "default_rule", "number_camera": 3, "time_range": 90, "camera_ids": []})
        logger.info(f"กำลังค้นหา key ใน Redis ด้วย pattern: '{redis_pattern}'")
        for key in redis_client.scan_iter(match=redis_pattern, count=1000):
            try:
                payload_str = redis_client.get(key)
                # print(f"payload_str: {payload_str}")
                if not payload_str: continue
                payload = json.loads(payload_str)
                if all(k in payload for k in ["id", "rule_name", "number_camera", "time_range", "camera_id"]):
                    rules.append({
                        "rule_id": payload["id"], "name": payload["rule_name"],
                        "number_camera": payload["number_camera"] if payload["number_camera"]>2 else 3,
                        "time_range": payload["time_range"] if payload["time_range"]<600 else 600,
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

def format_timestamp(ts):
    if isinstance(ts, datetime.datetime):
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
        return datetime.datetime.fromisoformat(raw_value)
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

def load_cached_events(spark, cache_path, lookback_start, logger):
    if not cache_path:
        return None
        
    logger.info(f"Loading parquet cache from {cache_path} (lookback_start={lookback_start})")
    
    try:
        df = spark.read.parquet(cache_path)
        
        # 1. Partition Pruning (Performance)
        # ตรวจสอบก่อนว่ามีคอลัมน์ partition ที่เราต้องการหรือไม่ เพื่อกัน error
        columns = df.columns
        if lookback_start and "event_date" in columns and "event_hour" in columns:
            start_date = lookback_start.date()
            start_hour = lookback_start.hour
            df = df.filter(
                (col("event_date") > lit(start_date)) |
                ((col("event_date") == lit(start_date)) & (col("event_hour") >= lit(start_hour)))
            )

        # 2. Precise Filtering (Accuracy)
        if lookback_start:
            df = df.filter(col("event_time") >= lit(lookback_start))
            
        logger.info(f"Successfully loaded and pruned cache from {cache_path}")
        return df

    except AnalysisException as exc:
        logger.warning(f"No parquet cache found at {cache_path}: {exc}")
        return None
    except Exception as exc:
        logger.error(f"Failed to read parquet cache from {cache_path}: {exc}", exc_info=True)
        return None

# def load_cached_events(spark, cache_path, min_event_time, logger):
#     if not cache_path:
#         return None
#     logger.info(
#         "Loading parquet cache from %s (min_event_time=%s).",
#         cache_path,
#         format_timestamp(min_event_time),
#     )
#     try:
#         cached_df = spark.read.parquet(cache_path)
#     except AnalysisException as exc:
#         logger.warning(f"No parquet cache found at {cache_path}: {exc}")
#         return None
#     except Exception as exc:
#         logger.error(f"Failed to read parquet cache from {cache_path}: {exc}", exc_info=True)
#         return None
#     logger.info("Loaded parquet cache from %s.", cache_path)
#     # logger.info("Parquet cache schema: %s", cached_df.schema.simpleString())
#     if min_event_time is not None:
#         cached_df = cached_df.filter(col("event_time") >= lit(min_event_time))
#     return cached_df

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

def send_to_kafka(spark, df, kafka_brokers, kafka_topic):
    """ส่ง DataFrame ไปยัง Kafka Topic"""
    if df.rdd.isEmpty():
        logger.info(f"DataFrame ว่างเปล่า ไม่มีการส่งข้อมูลไปยัง Kafka topic '{kafka_topic}'")
        return
    try:
        logger.info(f"กำลังพยายามส่งข้อมูลไปยัง Kafka topic '{kafka_topic}'...")
        kafka_output_df = df.select(to_json(struct("*")).alias("value"))
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
    # Start timer
    job_start_time = datetime.datetime.now()
    logger.info(f"เริ่มการทำงาน Job เวลา: {job_start_time}")
    
    args = parse_arguments()
    logger.info(f"ได้รับพารามิเตอร์: PG={args.postgres_host}, Kafka={args.kafka_brokers}, Lookback={args.lookback_hours}h")
    
    spark = None
    try:
        spark = create_spark_session()
        
        processing_timestamp = spark.sql("SELECT current_timestamp()").collect()[0][0]
        # เปลี่ยนชื่อตัวแปรให้สื่อความหมายมากขึ้น
        alert_time_cutoff = processing_timestamp - datetime.timedelta(seconds=int(args.time_threshold_seconds))
        logger.info(f"เวลาที่ใช้ประมวลผล (จาก Spark): {processing_timestamp}. จะกรอง Alert ที่มี end_time หลังจาก: {alert_time_cutoff}")
        jdbc_url = f"jdbc:postgresql://{args.postgres_host}:{args.postgres_port}/{args.postgres_db}"
        list_data = get_rule_data(args.redis_host, args.redis_port, args.redis_password, args.redis_pattern)
        list_data = sorted(
            list_data,
            key=lambda rule: len(rule.get("camera_ids") or []),
        )
        
        # --- CACHING LOGIC START ---
        
        # 1. Connect to Redis for Checkpoint (แยก connection สำหรับส่วนนี้)
        try:
            r_check = redis.Redis(
                host=args.redis_host, 
                port=args.redis_port, 
                password=args.redis_password, 
                decode_responses=True
            )
            r_check.ping()
        except Exception as e:
            logger.error(f"Failed to connect to Redis for checkpoint: {e}", exc_info=True)
            spark.stop()
            return

        cache_enabled = bool(args.parquet_cache_path)
        checkpoint_ts = None
        lookback_start = processing_timestamp - datetime.timedelta(hours=args.lookback_hours)

        if cache_enabled:
            checkpoint_ts = get_checkpoint_timestamp(r_check, args.redis_checkpoint_key, logger)
            if checkpoint_ts:
                delta = (processing_timestamp - checkpoint_ts).total_seconds()
                logger.info(f"Checkpoint found: {format_timestamp(checkpoint_ts)} (delta: {delta:.0f}s)")
            checkpoint_ts = clamp_checkpoint(checkpoint_ts, processing_timestamp, logger)

        query_start_ts = lookback_start
        if cache_enabled and checkpoint_ts:
            overlap = max(args.checkpoint_overlap_seconds, 0)
            query_start_ts = checkpoint_ts - datetime.timedelta(seconds=overlap)
            if query_start_ts < lookback_start:
                query_start_ts = lookback_start
        
        logger.info(f"Query Start Timestamp: {format_timestamp(query_start_ts)}")

        # 2. Load NEW data from PostgreSQL
        query_start_str = format_timestamp(query_start_ts)
        dbtable_query_new = (
            f"(SELECT license_plate, camera_id, camera_name, event_time "
            f" FROM {args.postgres_table} "
            f" WHERE event_time >= '{query_start_str}'"
            f") AS new_data"
        )
        
        try:
            new_events_df = (
                spark.read
                .format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", dbtable_query_new)
                .option("user", args.postgres_user)
                .option("password", args.postgres_password)
                .option("driver", "org.postgresql.Driver")
                .option("fetchsize", "10000")
                .option("application_name", 'vehicle_graph_analysis_incremental')
                .load()
                .withColumn("event_hour", hour("event_time"))
                .withColumn("event_date", col("event_time").cast("date"))
            ).cache()
            
            new_events_count = new_events_df.count()
            logger.info(f"Loaded {new_events_count} new records from PostgreSQL.")
            log_event_date_stats(new_events_df, logger, "postgres_new_events")

        except Exception as e:
            logger.error(f"Failed to load data from PostgreSQL: {e}", exc_info=True)
            spark.stop()
            return

        # 3. Load Cache
        cached_df = None
        if cache_enabled:
            cached_df = load_cached_events(spark, args.parquet_cache_path, lookback_start, logger)
            if cached_df:
                log_event_date_stats(cached_df, logger, "cached_events")

        # 4. Union
        if cached_df is not None:
            df_master = cached_df.unionByName(new_events_df, allowMissingColumns=True)
            logger.info("Using cached parquet data + new postgres data.")
        else:
            df_master = new_events_df
            logger.info("Using only postgres data.")
        
        # Filter to strict lookback window
        df_master = df_master.filter(col("event_time") >= lit(lookback_start)).cache()
        total_records = df_master.count()
        logger.info(f"Total Master Data ready for processing: {total_records} records.")

        # 5. Write Cache (如果มีข้อมูลใหม่)
        if cache_enabled and new_events_count > 0:
            try:
                # Materialize new events only for writing
                df_to_cache = new_events_df.select("*") # new_events_df is already cached
                logger.info(f"Appending {new_events_count} records to parquet cache at {args.parquet_cache_path}...")
                
                (df_to_cache
                    .write
                    .format("parquet")
                    .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "true")
                    .mode("append")
                    .partitionBy("event_date", "event_hour")
                    .save(args.parquet_cache_path))
                
                logger.info("Cache append successful.")
                log_cache_listing(spark, args.parquet_cache_path, logger)
                
                # Update Checkpoint
                max_event_time = (
                    new_events_df.agg(spark_max("event_time").alias("max_val"))
                    .collect()[0]["max_val"]
                )
                safe_checkpoint = clamp_checkpoint(max_event_time, processing_timestamp, logger, label="Max event_time")
                update_checkpoint_timestamp(r_check, args.redis_checkpoint_key, safe_checkpoint, logger)
                
            except Exception as e:
                logger.error(f"Failed to update parquet cache: {e}", exc_info=True)
        
        # Unpersist new_events_df as we now rely on df_master
        # new_events_df.unpersist() 
        # (Actually, df_master depends on new_events_df if uncached, but we cached df_master. 
        # If df_master is cached, we can unpersist new_events_df to free memory, but spark is smart strictly. 
        # Let's keep it simple for now.)

        # --- CACHING LOGIC END ---

        for rule in list_data:
            rule_id, name, number_camera, time_range, camera_ids = rule.values()
            
            logger.info(f"เริ่มประมวลผล Rule: '{name}' ({rule_id})")

            try:
                # Filter ข้อมูลจาก Memory (df_master) แทนการ Query ใหม่
                if camera_ids:
                    df_full = df_master.filter(col("camera_id").isin(camera_ids))
                    # ไม่ต้อง Cache ย่อยตรงนี้ เพราะ df_master cache อยู่แล้ว และการ filter partition ใน memory เร็วพอ
                else:
                    df_full = df_master
                
                rule_record_count = df_full.count()
                if rule_record_count == 0:
                    logger.warning(f"ไม่พบข้อมูลสำหรับ rule '{name}' (จาก Master Data). Skipping.")
                    continue
                logger.info(f"ข้อมูลสำหรับ rule '{name}': {rule_record_count} records")



                df = df_full.select("license_plate", "camera_name", "event_time").withColumn("timestamp_utc", unix_timestamp(col("event_time")))
                current_time_unix = unix_timestamp(current_timestamp())
                # OPTIMIZATION: แยกหา Active List เอาไว้ Filter ตอนหลัง
                cutoff_time = current_time_unix - args.time_threshold_seconds
                
                # 1. หา List ทะเบียนรถที่มีความเคลื่อนไหวในช่วงเวลาที่กำหนด (Active Cars)
                # เพื่อเอาไว้ Filter Groups ตอนจบ ว่าต้องมีรถในกลุ่มนี้อย่างน้อย 1 คัน
                df_interest = (
                    df
                    .filter(col("timestamp_utc") >= cutoff_time)
                    .select("license_plate")
                    .distinct()
                    .cache()
                )

                active_count = df_interest.count()
                if active_count == 0:
                    logger.info(f"ไม่พบรถที่ Active ในช่วง {args.time_threshold_seconds}s ล่าสุด สำหรับ rule '{name}'. Skipping.")
                    continue

                total_vehicle_count = df_full.select("license_plate").distinct().count()
                inactive_count = max(total_vehicle_count - active_count, 0)
                logger.info(f"พบ Active Cars จำนวน {active_count} คัน (ไว้ใช้ Filter กลุ่ม)")
                logger.info(
                    f"Inactive Cars จำนวน {inactive_count} คัน (จากทั้งหมด {total_vehicle_count} คัน) สำหรับ rule '{name}'"
                )
                df_interest_join = df_interest.withColumnRenamed("license_plate", "vehicle")
                if active_count < 1000:
                    df_interest_join = broadcast(df_interest_join)

                # 2. ใช้ข้อมูล Full 24H สำหรับสร้าง Graph (Events) เพื่อให้เห็นความสัมพันธ์ครบถ้วน
                # (ไม่ filter เวลาทิ้ง เพื่อให้เห็นคู่ Inactive)
                events = (
                    df
                    .withColumnRenamed("license_plate", "vehicle")
                    .withColumnRenamed("camera_name", "point")
                    .withColumnRenamed("timestamp_utc", "timestamp")
                    .repartition("point") 
                    .cache()
                )

                time_range_seconds = int(time_range)
                window_spec = (
                    Window.partitionBy("point")
                    .orderBy("timestamp")
                    .rangeBetween(-time_range_seconds, time_range_seconds)
                )
                edges_candidates = events.withColumn(
                    "candidates",
                    collect_set(struct("vehicle", "timestamp")).over(window_spec),
                )
                raw_edges = (
                    edges_candidates
                    .select(
                        col("vehicle").alias("src"),
                        col("point"),
                        col("timestamp"),
                        explode("candidates").alias("candidate"),
                    )
                    .filter(col("src") < col("candidate.vehicle"))
                    .filter(abs(col("candidate.timestamp") - col("timestamp")) <= time_range_seconds)
                    .select(
                        col("src"),
                        col("candidate.vehicle").alias("dst"),
                        col("point"),
                    )
                )
                logger.info(f"สร้าง Raw Edges สำหรับ rule '{name}' จำนวน {raw_edges.count()} รายการ")
                # edges = (
                #     raw_edges
                #     .groupBy("src", "dst")
                #     .agg(countDistinct("point").alias("common_points"))
                #     .filter(col("common_points") >= number_camera)
                # )
                edges_aggregated = (
                    raw_edges
                    .groupBy("src", "dst")
                    .agg(countDistinct("point").alias("common_points"))
                )

                # 2. **สำคัญมาก** ต้อง .cache() เพราะเราจะใช้ DataFrame นี้ 2 ครั้ง 
                # (ครั้งที่ 1 หา Max, ครั้งที่ 2 เอาไป Filter) ถ้าไม่ Cache Spark จะคำนวณใหม่ 2 รอบทำให้ช้า
                edges_aggregated.cache()

                # 3. สั่ง Action เพื่อหาค่า Max มา Log
                try:
                    # ตรวจสอบว่ามีข้อมูลหรือไม่ก่อนหา Max
                    if not edges_aggregated.rdd.isEmpty():
                        max_cp_row = edges_aggregated.agg(spark_max("common_points").alias("max_val")).collect()
                        max_common_points = max_cp_row[0]["max_val"]
                        
                        logger.info(f"STATS Rule '{name}': พบ common_points สูงสุด = {max_common_points} (เกณฑ์ที่ตั้งไว้: {number_camera})")
                    else:
                        logger.info(f"STATS Rule '{name}': ไม่พบ Edges ใดๆ เลยหลังจากการ Group")
                except Exception as e:
                    logger.warning(f"ไม่สามารถคำนวณ Max common points ได้: {e}")

                # 4. นำ edges_aggregated มา Filter ตามปกติ เพื่อไปทำงานต่อ
                edges = edges_aggregated.filter(col("common_points") >= number_camera)
                edges = edges.checkpoint(eager=True).cache()
                if edges.rdd.isEmpty():
                    logger.warning(f"ไม่พบ Edge ที่ตรงตามเงื่อนไขสำหรับ rule '{name}'.")
                    continue
                logger.info(f"กรอง Edges ที่มีอย่างน้อย {number_camera} common points สำหรับ rule '{name}' จำนวน {edges.count()} รายการ")
                verts = events.select(col("vehicle").alias("id")).distinct()
                g = GraphFrame(verts, edges.select("src", "dst"))
                cc = g.connectedComponents()
                groups_all = cc.groupBy("component").agg(collect_list("id").alias("vehicles")).filter(size(col("vehicles")) > 1)
                logger.info(f"พบกลุ่มรถทั้งหมด {groups_all.count()} กลุ่มสำหรับ rule '{name}'")
                groups_exploded = groups_all.select(col("component"), explode(col("vehicles")).alias("vehicle"))
                if active_count < 1000:
                    groups_with_interest = (
                        groups_exploded
                        .join(df_interest_join, "vehicle", "semi")
                        .select("component")
                        .distinct()
                        .join(groups_all, "component")
                    )
                else:
                    groups_with_interest = (
                        groups_exploded
                        .join(df_interest_join, "vehicle", "inner")
                        .select("component")
                        .distinct()
                        .join(groups_all, "component")
                    )
                if groups_with_interest.rdd.isEmpty():
                    logger.info(f"ไม่พบกลุ่มที่มีรถที่น่าสนใจสำหรับ rule '{name}'.")
                    continue
                logger.info(f"พบ {groups_with_interest.count()} กลุ่มที่น่าสนใจสำหรับ rule '{name}'. กำลังเตรียมสร้าง Alerts")
                
                all_vehicles_in_groups = groups_with_interest.select(col("component"), explode(col("vehicles")).alias("license_plate"))
                detailed_group_events = all_vehicles_in_groups.join(df_full, "license_plate", "inner")

                final_alerts_aggregated = detailed_group_events.groupBy("component").agg(
                    collect_set("license_plate").alias("license_plate"),
                    spark_min("event_time").alias("start_time"),
                    spark_max("event_time").alias("end_time"),
                    collect_set("camera_id").alias("cameras")
                )
                
                logger.info(f"กำลังกรอง Alerts โดยใช้เวลาอ้างอิง: {alert_time_cutoff}")
                final_alerts_filtered = final_alerts_aggregated.filter(
                    col("end_time") >= alert_time_cutoff
                )

                if final_alerts_filtered.rdd.isEmpty():
                    logger.info(f"ไม่พบกลุ่มที่น่าสนใจหลังจากการกรองเวลาสำหรับ rule '{name}'.")
                    continue

                # <<< เปลี่ยนแปลง: เพิ่ม lit(rule_id) เข้าไปใน select statement
                final_alerts = final_alerts_filtered.select(
                    lit(rule_id).alias("rule_id"), 
                    col("license_plate"),
                    date_format(col("start_time"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("start_time"),
                    date_format(col("end_time"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("end_time"),
                    col("cameras")
                )
                
                logger.info(f"สร้าง Group Alerts ทั้งหมด {final_alerts.count()} รายการสำหรับ rule '{name}' (หลังกรองเวลา)")
                
                if args.kafka_brokers:
                    send_to_kafka(spark, final_alerts, args.kafka_brokers, args.kafka_alerts_topic)

            except Exception as e:
                logger.error(f"เกิดข้อผิดพลาดระหว่างประมวลผล Graph สำหรับ rule '{name}': {str(e)}", exc_info=True)
            finally:
                # df_full.unpersist() # ไม่ต้อง unpersist เพราะเป็น slice จาก df_master
                if 'df_interest' in locals(): df_interest.unpersist()
                if 'events' in locals(): events.unpersist()
                if 'edges' in locals(): edges.unpersist()

        # Unpersist Master Data หลังจบทุก Rule
        df_master.unpersist()

    except Exception as e:
        logger.error(f"เกิดข้อผิดพลาดร้ายแรงในการทำงานหลัก: {str(e)}", exc_info=True)
    finally:
        if spark:
            logger.info("กำลังหยุด SparkSession...")
            spark.stop()
            logger.info("หยุด SparkSession สำเร็จ")
        
        # End timer and log duration
        job_end_time = datetime.datetime.now()
        duration = job_end_time - job_start_time
        logger.info(f"จบการทำงาน Job เวลา: {job_end_time}")
        logger.info(f"ระยะเวลาการทำงานทั้งหมด: {duration}")

if __name__ == "__main__":
    main()
