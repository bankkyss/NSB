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
###tst
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
SCRIPT_PATH = "/airflow-git/NSB.git/jobs/python/vehicle_area_analysis.py"

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

# รวมทุก Packages เป็น String เดียว
# การเพิ่ม kafka-clients เข้าไปโดยตรงจะช่วยแก้ปัญหา NoClassDefFoundError
ALL_PACKAGES_FOR_CONF = f"{SPARK_KAFKA_SQL_COORD},{KAFKA_CLIENTS_COORD},{POSTGRES_JDBC_COORD},{GRAPHFRAMES_COORD}"

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
    lookback_hours = Variable.get("lpr_lookback_hours", "12")
    time_threshold = Variable.get("lpr_time_threshold_seconds", "300")

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
except (AirflowNotFoundException, KeyError) as e:
    log.warning(f"Could not find a required Airflow Connection or Variable: {e}. "
                "The DAG will be created but tasks may fail. Please configure them in the UI.")
    APPLICATION_ARGS = []


# ---------------------------------------------------------------------------
# นิยาม DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="vehicle_area_analysis_production",
    description="วิเคราะห์การตัดกันของป้ายทะเบียนในพื้นที่ต่างๆ โดยใช้ Spark",
    start_date=datetime(2025, 6, 27),
    schedule="*/5 * * * *",  
    catchup=False,
    tags=["spark", "area", "vehicle", "production"],
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

    run_spark_job = SparkSubmitOperator(
        task_id="run_vehicle_area_analysis_job",
        application=SCRIPT_PATH,
        conn_id=SPARK_CONN_ID,
        name="vehicle_area_analysis_{{ ds_nodash }}_{{ ts_nodash }}",
        
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
        conf={
            # การกำหนดพื้นฐาน
            "spark.driver.host": spark_driver_ip,
            "spark.driver.cores": "1", 
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.dynamicAllocation.enabled": "false",
            
            
            # การตั้งค่าประสิทธิภาพ
            "spark.sql.adaptive.enabled": "false",
            
            # !! แก้ไขสำคัญ: กำหนด packages ที่นี่เพื่อความแน่นอน !!
            # วิธีนี้จะบังคับให้ Spark resolve และกระจาย JARs ไปยังทุก executor
            "spark.jars.packages": ALL_PACKAGES_FOR_CONF,
            
            # (ทางเลือก) ระบุ repository เพิ่มเติมหากจำเป็น
            "spark.jars.repositories": "https://repos.spark-packages.org/",
        },
        
        application_args=APPLICATION_ARGS,
        verbose=True,
    )

    check_pyspark >> check_cluster >> run_spark_job