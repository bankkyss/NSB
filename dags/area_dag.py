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
GRAPHFRAMES_COORD = "graphframes:graphframes:0.8.4-spark3.5-s_2.12"
POSTGRES_JDBC_COORD = "org.postgresql:postgresql:42.2.19"
SPARK_KAFKA_COORD = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
ALL_PACKAGES = f"{GRAPHFRAMES_COORD},{POSTGRES_JDBC_COORD},{SPARK_KAFKA_COORD}"

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
        masters = conn.host.removeprefix("spark://").split(",")
        reachable = []
        for host_port in masters:
            host = host_port.split(":")[0]
            url = f"http://{host}:80"  
            try:
                r = requests.get(url, timeout=5)
                if r.status_code == 200:
                    log.info(f"Successfully reached Spark master at {url}")
                    reachable.append(url)
            except requests.RequestException as e:
                log.warning(f"Could not reach Spark master at {url}. Error: {e}")
        if not reachable:
            raise ConnectionError("No reachable Spark masters found!")
    except AirflowNotFoundException:
        log.error(f"Airflow connection '{SPARK_CONN_ID}' not found.")
        raise
    except Exception as e:
        log.error(f"An unexpected error occurred during health check: {e}")
        raise

# ---------------------------------------------------------------------------
# ดึง Configurations จาก Airflow Connections และ Variables
# ---------------------------------------------------------------------------
# ใช้ try-except เพื่อป้องกัน DAG หยุดทำงานหากยังไม่ได้ตั้งค่า Variable
try:
    pg_conn = BaseHook.get_connection(POSTGRES_CONN_ID)
    redis_conn = BaseHook.get_connection(REDIS_CONN_ID)
    
    # ดึงค่าจาก Airflow Variables (สามารถไปตั้งค่าได้ที่ Admin -> Variables)
    kafka_brokers = Variable.get("kafka_brokers", "kafka.kafka.svc.cluster.local:9092")
    kafka_alerts_topic = Variable.get("kafka_alerts_topic", "alearts_topic")
    kafka_log_event_topic = Variable.get("kafka_log_event_topic", "log_event_topic")
    lookback_hours = Variable.get("lpr_lookback_hours", "12")
    time_threshold = Variable.get("lpr_time_threshold_seconds", "300")

    # สร้าง List ของ Arguments ที่จะส่งให้ PySpark Script
    # วิธีนี้ทำให้เราไม่ต้อง Hardcode ค่าใดๆ ในสคริปต์หลักเลย
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
except AirflowNotFoundException as e:
    log.warning(f"Could not find a required Airflow Connection or Variable: {e}. "
                "The DAG will be created but tasks may fail. Please configure them in the UI.")
    APPLICATION_ARGS = [] # ตั้งเป็น list ว่างเพื่อไม่ให้ DAG แตก


# ---------------------------------------------------------------------------
# นิยาม DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="vehicle_area_analysis_production",
    description="วิเคราะห์การตัดกันของป้ายทะเบียนในพื้นที่ต่างๆ โดยใช้ Spark",
    start_date=datetime(2025, 6, 27),
    schedule="*/5 * * * *",  # รันทุก 5 นาที
    catchup=False,
    tags=["spark", "area", "vehicle", "production"],
    max_active_runs=1,  # ป้องกันการรันซ้อนกันมากเกินไป
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

    # Task หลักสำหรับรัน Spark Job
    run_spark_job = SparkSubmitOperator(
        task_id="run_vehicle_area_analysis_job",
        application=SCRIPT_PATH,
        conn_id=SPARK_CONN_ID,
        name="vehicle_area_analysis_{{ ds_nodash }}_{{ ts_nodash }}",
        packages=ALL_PACKAGES,
        
        # --- การปรับแต่งประสิทธิภาพ Spark (Final Configuration) ---
        # เป้าหมาย: 15 Cores และไม่เกิน 32GB RAM

        # !! ตัวแปรสำคัญที่สุด บังคับให้ Master จอง core สำหรับ Executor แค่ 14 !!
        total_executor_cores=14, 
        
        # พารามิเตอร์ด้านล่างจะถูกใช้เพื่อแบ่งสรร 14 cores ที่ได้มา
        num_executors=2,
        executor_cores=7, # (14 cores / 2 executors = 7 cores/executor)
        
        executor_memory="14g",
        driver_memory="4g",
        driver_cores=1, # รวมกับ total_executor_cores แล้วจะเป็น 15
        
        # Spark configurations เพิ่มเติม
        conf={
            "spark.dynamicAllocation.enabled": "false",
            "spark.sql.adaptive.enabled": "false",
        },
        
        application_args=APPLICATION_ARGS,
        verbose=True,
    )

    # กำหนดลำดับการทำงานของ Tasks
    check_pyspark >> check_cluster >> run_spark_job
