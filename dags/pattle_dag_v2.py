# -*- coding: utf-8 -*-
"""
### Vehicle Graph Analysis DAG

DAG นี้ทำหน้าที่ประสานงาน Spark job เพื่อวิเคราะห์ความสัมพันธ์ของยานพาหนะ
โดยใช้ GraphFrames สำหรับการวิเคราะห์ Co-occurrence ของยานพาหนะ โดยมีขั้นตอนดังนี้:
1.  ตรวจสอบเวอร์ชันของ PySpark และความพร้อมของ Spark Cluster
2.  ดึงค่า Configuration ทั้งหมดจาก Airflow Connections และ Variables เพื่อความปลอดภัยและยืดหยุ่น
3.  ส่ง Spark job (`vehicle_graph_analysis_2.py`) พร้อมกับค่า Configuration ที่ดึงมา
4.  Job จะอ่านข้อมูลจาก PostgreSQL, ประมวลผลด้วย GraphFrames,
    และส่งผลลัพธ์ไปยัง Kafka
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

import requests
from airflow.hooks.base import BaseHook
from airflow.models.dag import DAG
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.exceptions import AirflowNotFoundException
import os
# import requests

# ---------------------------------------------------------------------------
# ค่าคงที่สำหรับ DAG - สามารถปรับเปลี่ยนได้ตามสภาพแวดล้อม
# ---------------------------------------------------------------------------
# ตำแหน่งของสคริปต์ PySpark ที่จะรัน
SCRIPT_PATH = "/airflow-git/NSB.git/jobs/python/vehicle_graph_analysisv2.py"

# Connection ID ของ Spark Master ใน Airflow
SPARK_CONN_ID = "spark"

# Connection ID ของ Database ใน Airflow
POSTGRES_CONN_ID = "postgres_lpr_db"
REDIS_CONN_ID = "redis_main"
# เวอร์ชัน PySpark ที่คาดหวัง
PYSPARK_EXPECTED_VERSION = "3.5.5"

# Spark Packages ที่จำเป็นสำหรับ Job
GRAPHFRAMES_COORD = "graphframes:graphframes:0.8.4-spark3.5-s_2.12"
POSTGRES_JDBC_COORD = "org.postgresql:postgresql:42.2.19"
SPARK_KAFKA_SQL_COORD = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
KAFKA_CLIENTS_COORD = "org.apache.kafka:kafka-clients:3.4.1"
SPARK_HADOOP_CLOUD_COORD = "org.apache.spark:spark-hadoop-cloud_2.12:3.5.5"
HADOOP_AWS_COORD = "org.apache.hadoop:hadoop-aws:3.3.4"
AWS_SDK_BUNDLE_COORD = "com.amazonaws:aws-java-sdk-bundle:1.12.262"

# รวมทุก Packages เป็น String เดียว
ALL_PACKAGES_FOR_CONF = (
    f"{GRAPHFRAMES_COORD},{SPARK_KAFKA_SQL_COORD},{KAFKA_CLIENTS_COORD},"
    f"{POSTGRES_JDBC_COORD},{SPARK_HADOOP_CLOUD_COORD},{HADOOP_AWS_COORD},{AWS_SDK_BUNDLE_COORD}"
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ฟังก์ชันสำหรับ PythonOperator (Pre-flight checks)
# ---------------------------------------------------------------------------

def verify_pyspark_version() -> None:
    """ตรวจสอบว่า PySpark version ตรงกับที่คาดหวังหรือไม่"""
    import pyspark
    ver = pyspark.__version__
    log.info(f"Current PySpark version: {ver}")
    if ver != PYSPARK_EXPECTED_VERSION:
        raise ValueError(
            f"Expected PySpark {PYSPARK_EXPECTED_VERSION}, but found {ver}"
        )

def check_spark_cluster_health() -> None:
    """ตรวจสอบว่าสามารถเข้าถึง Spark Master UI ได้หรือไม่"""
    log.info(f"Checking Spark master health for connection: {SPARK_CONN_ID}")
    try:
        conn = BaseHook.get_connection(SPARK_CONN_ID)
        # ใช้ conn.host โดยตรง
        masters = conn.host.split(",")
        reachable = []
        for host_port in masters:
            # ใช้ host จาก connection ตรงๆ และกำหนด port ของ Web UI
            host = host_port.replace("spark://", "").split(":")[0]
            url = f"http://{host}:80"  # Spark Master Web UI ปกติใช้พอร์ต 8080
            try:
                # ใช้ HEAD request เพื่อความรวดเร็ว
                r = requests.head(url, timeout=5)
                r.raise_for_status() # ตรวจสอบ status code ที่เป็น 2xx
                log.info(f"Successfully reached Spark master UI at {url}")
                reachable.append(url)
            except requests.RequestException as e:
                log.warning(f"Could not reach Spark master UI at {url}. Error: {e}")
        if not reachable:
            raise ConnectionError("No reachable Spark master UIs found!")
    except AirflowNotFoundException:
        log.error(f"Airflow connection '{SPARK_CONN_ID}' not found.")
        raise
    except Exception as e:
        log.error(f"An unexpected error occurred during health check: {e}")
        raise

# ---------------------------------------------------------------------------
# ดึง Configurations จาก Airflow Connections และ Variables
# ---------------------------------------------------------------------------
try:
    pg_conn = BaseHook.get_connection(POSTGRES_CONN_ID)
    redis_conn = BaseHook.get_connection(REDIS_CONN_ID)
    
    kafka_brokers = Variable.get("kafka_brokers", "kafka.kafka.svc.cluster.local:9092")
    kafka_alerts_topic = Variable.get("kafka_alerts_topic_pattle", "alearts_topic")
    kafka_log_event_topic = Variable.get("kafka_log_event_topic", "log_event_topic")
    lookback_hours = Variable.get("lpr_lookback_hours_graph", "12")
    time_threshold = Variable.get("graph_time_threshold_seconds_v2", "300")
    
    # Arguments สำหรับ Parquet Caching
    parquet_cache_path = Variable.get("vehicle_graph_parquet_cache_path", "")
    redis_checkpoint_key = Variable.get("vehicle_graph_checkpoint_key", "vehicle_graph_analysis:checkpoint_ts")
    checkpoint_overlap_seconds = Variable.get("vehicle_graph_checkpoint_overlap_seconds", "60")
    
    rustfs_endpoint = Variable.get("rustfs_s3_endpoint", "")
    rustfs_access_key = Variable.get("rustfs_access_key", "")
    rustfs_secret_key = Variable.get("rustfs_secret_key", "")
    rustfs_path_style = Variable.get("rustfs_s3_path_style", "true")
    rustfs_ssl_enabled = Variable.get("rustfs_s3_ssl_enabled", "false")
    spark_maven_repositories = Variable.get(
        "spark_maven_repositories",
        "https://repo.maven.apache.org/maven2/,https://repo1.maven.org/maven2/,https://repos.spark-packages.org/",
    )

    APPLICATION_ARGS = [
        "--postgres-host", pg_conn.host,
        "--postgres-port", str(pg_conn.port),
        "--postgres-db", pg_conn.schema,
        "--postgres-user", pg_conn.login,
        "--postgres-password", pg_conn.password,
        "--postgres-table", "vehicle_events",
        "--redis-host", redis_conn.host,
        "--redis-port", str(redis_conn.port),
        "--redis-password", redis_conn.password,
        "--kafka-brokers", kafka_brokers,
        "--kafka-alerts-topic", f"{kafka_alerts_topic}demo",
        "--kafka-log-event-topic", f"{kafka_log_event_topic}demo",
        "--lookback-hours", lookback_hours,
        "--time-threshold-seconds", time_threshold,
        "--parquet-cache-path", parquet_cache_path,
        "--redis-checkpoint-key", redis_checkpoint_key,
        "--checkpoint-overlap-seconds", checkpoint_overlap_seconds,
    ]
except (AirflowNotFoundException, KeyError) as e:
    log.warning(f"Could not find a required Airflow Connection or Variable: {e}. "
                "The DAG will be created but tasks may fail. Please configure them in the UI.")
    APPLICATION_ARGS = []

# ---------------------------------------------------------------------------
# นิยาม DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="vehicle_graph_analysis_production_v2",
    description="วิเคราะห์ความสัมพันธ์ของยานพาหนะโดยใช้ GraphFrames และ Spark (v2)_fortest",
    start_date=datetime(2025, 6, 27),
    schedule="*/15 * * * *",  # รันทุกๆ 10 นาที
    catchup=False,
    tags=["spark", "graphframes", "vehicle", "production"],
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=60),
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:

    check_pyspark = PythonOperator(
        task_id="check_pyspark_version",
        python_callable=verify_pyspark_version,
    )

    check_cluster = PythonOperator(
        task_id="check_spark_cluster_health",
        python_callable=check_spark_cluster_health,
    )

    # ควรดึงค่า IP ของ pod ภายใน task เพื่อให้ได้ค่าล่าสุดเสมอ
    spark_driver_ip = os.environ.get("SPARK_DRIVER_POD_IP", "0.0.0.0")

    run_spark_job = SparkSubmitOperator(
        task_id="run_vehicle_graph_analysis_job",
        application=SCRIPT_PATH,
        conn_id=SPARK_CONN_ID,
        name="vehicle_graph_analysis_v2_{{ ds_nodash }}_{{ ts_nodash }}",
        
        # --- การปรับแต่งประสิทธิภาพ Spark สำหรับ Graph Analysis ---
        # total_executor_cores=10,
        total_executor_cores=20, 
        # พารามิเตอร์สำหรับแบ่งสรร 10 Cores ที่ได้มา
        num_executors=2,
        executor_cores=6,
        executor_memory="10g",
        driver_memory="8g",
        
        # --- Spark configurations เพิ่มเติม ---
        conf={
            # การกำหนดพื้นฐาน
            "spark.driver.host": spark_driver_ip,
            "spark.driver.cores": "1",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.dynamicAllocation.enabled": "false",
            
            # การตั้งค่าประสิทธิภาพสำหรับ GraphFrames
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.autoBroadcastJoinThreshold": "100m",
            "spark.sql.broadcastTimeout": "600",
            "spark.sql.shuffle.partitions": "200",
            "spark.sql.files.maxPartitionBytes": "128m",
            "spark.executor.memoryOverhead": "4g",
            "spark.driver.memoryOverhead": "2g",
            "spark.driver.maxResultSize": "2g",
            "spark.sql.maxPlanStringLength": "52428800",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.streaming.forceDeleteTempCheckpointLocation": "true",
            
            # เพิ่ม timeout settings
            "spark.network.timeout": "800s",
            "spark.executor.heartbeatInterval": "60s",
            "spark.sql.execution.arrow.pyspark.enabled": "false",
            
            # !! กำหนด packages ที่นี่เพื่อความแน่นอน !!
            "spark.jars.packages": ALL_PACKAGES_FOR_CONF,
            
            # ระบุ repository เพิ่มเติมเพื่อให้แน่ใจว่าหา packages เจอ
            "spark.jars.repositories": spark_maven_repositories,

            # S3 / RustFS configurations for Parquet caching
            "spark.hadoop.fs.s3a.endpoint": rustfs_endpoint,
            "spark.hadoop.fs.s3a.access.key": rustfs_access_key,
            "spark.hadoop.fs.s3a.secret.key": rustfs_secret_key,
            "spark.hadoop.fs.s3a.path.style.access": rustfs_path_style,
            "spark.hadoop.fs.s3a.connection.ssl.enabled": rustfs_ssl_enabled,
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.change.detection.mode": "none",
            "spark.hadoop.fs.s3a.change.detection.source": "none",
            "spark.hadoop.fs.s3a.change.detection.version.required": "false",

            # เพิ่มค่า Timeout สำหรับ HDFS Client
            "spark.hadoop.ipc.client.connect.timeout": "60000", # 60 วินาที (จากเดิมมักจะ 20s)
            "spark.hadoop.ipc.client.connect.max.retries.on.timeouts": "5",
            "spark.hadoop.ipc.ping.interval": "30000",
            "spark.hadoop.dfs.client.socket-timeout": "120000", # เพิ่ม Socket timeout
        },
        
        application_args=APPLICATION_ARGS,
        verbose=True,
    )

    check_pyspark >> check_cluster >> run_spark_job
