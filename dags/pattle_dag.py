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

# ---------------------------------------------------------------------------
# ค่าคงที่สำหรับ DAG - สามารถปรับเปลี่ยนได้ตามสภาพแวดล้อม
# ---------------------------------------------------------------------------
# ตำแหน่งของสคริปต์ PySpark ที่จะรัน
SCRIPT_PATH = "/airflow-git/NSB.git/jobs/python/vehicle_graph_analysis.py"

# Connection ID ของ Spark Master ใน Airflow
SPARK_CONN_ID = "spark"

# Connection ID ของ Database ใน Airflow
POSTGRES_CONN_ID = "postgres_lpr_db"

# เวอร์ชัน PySpark ที่คาดหวัง
PYSPARK_EXPECTED_VERSION = "3.5.5"

# Spark Packages ที่จำเป็นสำหรับ Job
GRAPHFRAMES_COORD = "graphframes:graphframes:0.8.4-spark3.5-s_2.12"
POSTGRES_JDBC_COORD = "org.postgresql:postgresql:42.2.19"
SPARK_KAFKA_SQL_COORD = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
KAFKA_CLIENTS_COORD = "org.apache.kafka:kafka-clients:3.4.1"

# รวมทุก Packages เป็น String เดียว
ALL_PACKAGES_FOR_CONF = f"{GRAPHFRAMES_COORD},{SPARK_KAFKA_SQL_COORD},{KAFKA_CLIENTS_COORD},{POSTGRES_JDBC_COORD}"

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
    
    kafka_brokers = Variable.get("kafka_brokers", "kafka.kafka.svc.cluster.local:9092")
    kafka_alerts_topic = Variable.get("kafka_alerts_topic", "alearts_topic")
    kafka_log_event_topic = Variable.get("kafka_log_event_topic", "log_event_topic")
    lookback_hours = Variable.get("graph_lookback_hours", "24")
    time_threshold = Variable.get("graph_time_threshold_seconds", "300")
    
    APPLICATION_ARGS = [
        "--postgres-host", pg_conn.host,
        "--postgres-port", str(pg_conn.port),
        "--postgres-db", pg_conn.schema,
        "--postgres-user", pg_conn.login,
        "--postgres-password", pg_conn.password,
        "--postgres-table", "vehicle_events",
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
    dag_id="vehicle_graph_analysis_production",
    description="วิเคราะห์ความสัมพันธ์ของยานพาหนะโดยใช้ GraphFrames และ Spark",
    start_date=datetime(2025, 6, 27),
    schedule="0 2 * * *",  # รันทุกวันเวลา 02:00 น.
    catchup=False,
    tags=["spark", "graphframes", "vehicle", "production"],
    max_active_runs=1,
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
        name="vehicle_graph_analysis_{{ ds_nodash }}_{{ ts_nodash }}",
        
        # --- การปรับแต่งประสิทธิภาพ Spark สำหรับ Graph Analysis ---
        total_executor_cores=10,
        
        # พารามิเตอร์สำหรับแบ่งสรร 10 Cores ที่ได้มา
        num_executors=2,
        executor_cores=5,
        executor_memory="10g",
        driver_memory="10g",
        
        # --- Spark configurations เพิ่มเติม ---
        conf={
            # การกำหนดพื้นฐาน
            # "spark.driver.host": spark_driver_ip,
            "spark.driver.cores": "1",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.dynamicAllocation.enabled": "false",
            
            # การตั้งค่าประสิทธิภาพสำหรับ GraphFrames
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.driver.maxResultSize": "2g",
            "spark.sql.maxPlanStringLength": "10485760",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.streaming.forceDeleteTempCheckpointLocation": "true",
            
            # เพิ่ม timeout settings
            "spark.network.timeout": "800s",
            "spark.executor.heartbeatInterval": "60s",
            "spark.sql.execution.arrow.pyspark.enabled": "false",
            
            # !! กำหนด packages ที่นี่เพื่อความแน่นอน !!
            "spark.jars.packages": ALL_PACKAGES_FOR_CONF,
            
            # ระบุ repository เพิ่มเติมหากจำเป็น
            "spark.jars.repositories": "https://repos.spark-packages.org/",
        },
        
        application_args=APPLICATION_ARGS,
        verbose=True,
    )

    check_pyspark >> check_cluster >> run_spark_job