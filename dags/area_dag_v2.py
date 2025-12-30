# -*- coding: utf-8 -*-
"""
### Vehicle Area Analysis DAG

DAG นี้ทำหน้าที่ประสานงาน Spark job เพื่อวิเคราะห์การปรากฏตัวของยานพาหนะ
ในพื้นที่ที่กำหนดจากกล้องหลายตัว โดยมีขั้นตอนดังนี้:
1.  ตรวจสอบเวอร์ชันของ PySpark และความพร้อมของ Spark Cluster
2.  ดึงค่า Configuration ทั้งหมดจาก Airflow Connections และ Variables เพื่อความปลอดภัยและยืดหยุ่น
3.  ส่ง Spark job (`vehicle_area_analysis.py`) พร้อมกับค่า Configuration ที่ดึงมา
4.  Job จะอ่านข้อมูลจาก PostgreSQL, อ่าน Area definitions จาก Redis, ประมวลผล,
    และส่งผลลัพธ์ (Alerts และ Logs) ไปยัง Kafka
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
# ---------------------------------------------------------------------------
# ค่าคงที่สำหรับ DAG - สามารถปรับเปลี่ยนได้ตามสภาพแวดล้อม
# ---------------------------------------------------------------------------
# ตำแหน่งของสคริปต์ PySpark ที่จะรัน
SCRIPT_PATH = "/airflow-git/NSB.git/jobs/python/vehicle_area_analysis_use_crash.py"

# Connection ID ของ Spark Master ใน Airflow
SPARK_CONN_ID = "spark"

# Connection ID ของ Database และ Redis ใน Airflow
POSTGRES_CONN_ID = "postgres_lpr_db"
REDIS_CONN_ID = "redis_main"

# เวอร์ชัน PySpark ที่คาดหวัง
PYSPARK_EXPECTED_VERSION = "3.5.5"

# Spark Packages ที่จำเป็นสำหรับ Job
# เราจะกำหนดค่านี้ใน 'conf' ของ SparkSubmitOperator เพื่อความแน่นอน
# ดู Log เดิมเพื่อหาเวอร์ชันที่เข้ากันได้
KAFKA_CLIENTS_COORD = "org.apache.kafka:kafka-clients:3.4.1"
SPARK_KAFKA_SQL_COORD = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
POSTGRES_JDBC_COORD = "org.postgresql:postgresql:42.2.19"
GRAPHFRAMES_COORD = "graphframes:graphframes:0.8.4-spark3.5-s_2.12"
SPARK_HADOOP_CLOUD_COORD = "org.apache.spark:spark-hadoop-cloud_2.12:3.5.5"
HADOOP_AWS_COORD = "org.apache.hadoop:hadoop-aws:3.3.4"
AWS_SDK_BUNDLE_COORD = "com.amazonaws:aws-java-sdk-bundle:1.12.262"

# รวมทุก Packages เป็น String เดียว
# การเพิ่ม kafka-clients เข้าไปโดยตรงจะช่วยแก้ปัญหา NoClassDefFoundError
ALL_PACKAGES_FOR_CONF = (
    f"{SPARK_KAFKA_SQL_COORD},{KAFKA_CLIENTS_COORD},{POSTGRES_JDBC_COORD},"
    f"{GRAPHFRAMES_COORD},{SPARK_HADOOP_CLOUD_COORD},{HADOOP_AWS_COORD},{AWS_SDK_BUNDLE_COORD}"
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
        # แก้ไข: ใช้ conn.host โดยตรง ไม่ต้องตัด prefix
        masters = conn.host.split(",")
        reachable = []
        for host_port in masters:
            # แก้ไข: ใช้ host จาก connection ตรงๆ และกำหนด port ของ Web UI
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
    kafka_alerts_topic = Variable.get("kafka_alerts_topic", "alearts_topic")
    kafka_log_event_topic = Variable.get("kafka_log_event_topic", "log_event_topic")
    kafka_alerts_topic = f"{kafka_alerts_topic}demo"
    kafka_log_event_topic = f"{kafka_log_event_topic}demo"
    lookback_hours = Variable.get("lpr_lookback_hours", "12")
    time_threshold = Variable.get("lpr_time_threshold_seconds", "300")
    parquet_cache_path = Variable.get("vehicle_area_parquet_cache_path", "")
    redis_checkpoint_key = Variable.get(
        "vehicle_area_checkpoint_key",
        "vehicle_area_analysis:checkpoint_ts",
    )
    checkpoint_overlap_seconds = Variable.get(
        "vehicle_area_checkpoint_overlap_seconds",
        "60",
    )
    rustfs_endpoint = Variable.get("rustfs_s3_endpoint", "")
    rustfs_access_key = Variable.get("rustfs_access_key", "")
    rustfs_secret_key = Variable.get("rustfs_secret_key", "")
    rustfs_path_style = Variable.get("rustfs_s3_path_style", "true")
    rustfs_ssl_enabled = Variable.get("rustfs_s3_ssl_enabled", "false")
    spark_maven_repositories = Variable.get(
        "spark_maven_repositories",
        "https://repo.maven.apache.org/maven2/,https://repo1.maven.org/maven2/,https://repos.spark-packages.org/",
    )
    spark_jars_ivy = Variable.get("spark_jars_ivy", "")

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
        "--kafka-alerts-topic", kafka_alerts_topic,
        "--kafka-log-event-topic", kafka_log_event_topic,
        "--lookback-hours", lookback_hours,
        "--time-threshold-seconds", time_threshold,
    ]
    if parquet_cache_path:
        APPLICATION_ARGS += [
            "--parquet-cache-path", parquet_cache_path,
            "--redis-checkpoint-key", redis_checkpoint_key,
            "--checkpoint-overlap-seconds", checkpoint_overlap_seconds,
        ]

    SPARK_CONF_BASE = {
        "spark.driver.cores": "1",
        "spark.driver.bindAddress": "0.0.0.0",
        "spark.dynamicAllocation.enabled": "false",
        "spark.sql.adaptive.enabled": "false",
        "spark.jars.packages": ALL_PACKAGES_FOR_CONF,
        "spark.jars.repositories": spark_maven_repositories,
    }
    if spark_jars_ivy:
        SPARK_CONF_BASE["spark.jars.ivy"] = spark_jars_ivy

    if parquet_cache_path.startswith("s3a://") and rustfs_endpoint and rustfs_access_key and rustfs_secret_key:
        SPARK_CONF_BASE.update(
            {
                "spark.hadoop.fs.s3a.endpoint": rustfs_endpoint,
                "spark.hadoop.fs.s3a.access.key": rustfs_access_key,
                "spark.hadoop.fs.s3a.secret.key": rustfs_secret_key,
                "spark.hadoop.fs.s3a.path.style.access": rustfs_path_style,
                "spark.hadoop.fs.s3a.connection.ssl.enabled": rustfs_ssl_enabled,
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.aws.credentials.provider": (
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
                ),
                "spark.hadoop.fs.s3a.committer.name": "directory",
                "spark.hadoop.fs.s3a.committer.staging.conflict-mode": "append",
                "spark.hadoop.fs.s3a.committer.staging.unique-filenames": "true",
                "spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a": (
                    "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory"
                ),
                "spark.sql.sources.commitProtocolClass": (
                    "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol"
                ),
                "spark.sql.parquet.output.committer.class": (
                    "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter"
                ),
                "spark.sql.sources.partitionColumnTypeInference.enabled": "true",
                "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
                "spark.hadoop.fs.s3a.change.detection.mode": "none",
                "spark.hadoop.fs.s3a.change.detection.source": "none",
            }
        )
except (AirflowNotFoundException, KeyError) as e:
    log.warning(f"Could not find a required Airflow Connection or Variable: {e}. "
                "The DAG will be created but tasks may fail. Please configure them in the UI.")
    APPLICATION_ARGS = []
    SPARK_CONF_BASE = {}


# ---------------------------------------------------------------------------
# นิยาม DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="vehicle_area_analysis_production_v2",
    description="วิเคราะห์การตัดกันของป้ายทะเบียนในพื้นที่ต่างๆ โดยใช้ Spark (v2)",
    start_date=datetime(2025, 6, 27),
    schedule="*/5 * * * *",  
    catchup=False,
    tags=["spark", "area", "vehicle", "production", "v2"],
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=30),
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=3),
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
    spark_conf = dict(SPARK_CONF_BASE)
    spark_conf["spark.driver.host"] = spark_driver_ip

    run_spark_job = SparkSubmitOperator(
        task_id="run_vehicle_area_analysis_job",
        application=SCRIPT_PATH,
        conn_id=SPARK_CONN_ID,
        name="vehicle_area_analysis_v2_{{ ds_nodash }}_{{ ts_nodash }}",
        
        # --- ลบ parameter 'packages' ออกจากตรงนี้ ---
        # packages=ALL_PACKAGES, 
        
        # --- การปรับแต่งประสิทธิภาพ Spark ---
        total_executor_cores=20, 

        # พารามิเตอร์สำหรับแบ่งสรร 14 Cores ที่ได้มา
        num_executors=1,
        executor_cores=14,
        executor_memory="25g",
        driver_memory="4g",
        
        # --- Spark configurations เพิ่มเติม (แก้ไขจุดนี้) ---
        conf=spark_conf,
        
        application_args=APPLICATION_ARGS,
        verbose=True,
    )

    check_pyspark >> check_cluster >> run_spark_job
