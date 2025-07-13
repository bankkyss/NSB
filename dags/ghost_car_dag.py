from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import requests
from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook
from airflow.models.dag import DAG
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# ---------------------------------------------------------------------------
# ค่าคงที่สำหรับ DAG - สามารถปรับเปลี่ยนได้ตามสภาพแวดล้อม
# ---------------------------------------------------------------------------
# ตำแหน่งของสคริปต์ PySpark ที่จะรัน (ควรอยู่ใน Git-sync หรือ Volume ที่เข้าถึงได้)
SCRIPT_PATH = "/airflow-git/NSB.git/jobs/python/vehicle_in_normally.py" 

# Connection ID ของ Spark, PostgreSQL, และ Redis ใน Airflow
SPARK_CONN_ID = "spark" # ใช้ชื่อ connection ของคุณ
POSTGRES_CONN_ID = "postgres_lpr_db"
REDIS_CONN_ID = "redis_main"

# Spark Packages ที่จำเป็นสำหรับ Job
# เราจะกำหนดค่านี้ใน 'conf' ของ SparkSubmitOperator เพื่อความแน่นอน
SPARK_KAFKA_SQL_COORD = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
POSTGRES_JDBC_COORD = "org.postgresql:postgresql:42.7.3" # แนะนำให้ใช้เวอร์ชันล่าสุด

# รวมทุก Packages เป็น String เดียว
ALL_PACKAGES_FOR_CONF = f"{SPARK_KAFKA_SQL_COORD},{POSTGRES_JDBC_COORD}"

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ฟังก์ชันสำหรับ PythonOperator (Pre-flight checks)
# ---------------------------------------------------------------------------

def check_spark_cluster_health() -> None:
    """ตรวจสอบว่าสามารถเข้าถึง Spark Master UI ได้หรือไม่"""
    log.info(f"Checking Spark master health for connection: {SPARK_CONN_ID}")
    try:
        conn = BaseHook.get_connection(SPARK_CONN_ID)
        # รองรับการระบุ master หลายตัวในรูปแบบ comma-separated
        masters = conn.host.split(",")
        reachable = []
        for host_port in masters:
            # แยก host และกำหนด port ของ Web UI (ปกติคือ 8080)
            host = host_port.replace("spark://", "").split(":")[0]
            url = f"http://{host}:80"
            try:
                # ใช้ HEAD request เพื่อความรวดเร็ว ไม่ต้องดึงเนื้อหาทั้งหมด
                r = requests.head(url, timeout=10)
                r.raise_for_status() # ตรวจสอบว่า status code เป็น 2xx
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
    
    # ดึงค่าจาก Airflow Variables (สามารถกำหนดค่า default ได้)
    kafka_brokers = Variable.get("kafka_brokers", "kafka.kafka.svc.cluster.local:9092")
    kafka_alerts_topic = Variable.get("kafka_alerts_topic", "alerts_topic")
    kafka_log_event_topic = Variable.get("kafka_log_event_topic", "log_event_topic")
    lookback_hours = Variable.get("lpr_lookback_hours", "12")
    osrm_url = Variable.get("osrm_server_url", "http://router.project-osrm.org")

    # สร้าง List ของ Arguments ที่จะส่งให้สคริปต์ PySpark
    APPLICATION_ARGS = [
        "--postgres-host", pg_conn.host,
        "--postgres-port", str(pg_conn.port),
        "--postgres-db", pg_conn.schema,
        "--postgres-user", pg_conn.login,
        "--postgres-password", pg_conn.password,
        "--postgres-table", "vehicle_events",
        "--redis-host", redis_conn.host,
        "--redis-port", str(redis_conn.port),
        "--redis-password", redis_conn.password or "", # ส่งค่าว่างถ้าไม่มี password
        "--kafka-brokers", kafka_brokers,
        "--kafka-alerts-topic", kafka_alerts_topic,
        "--kafka-log-event-topic", kafka_log_event_topic,
        "--lookback-hours", lookback_hours,
        "--osrm-server-url", osrm_url, # เพิ่ม Argument สำหรับ OSRM
    ]
except (AirflowNotFoundException, KeyError) as e:
    log.warning(f"Could not find a required Airflow Connection or Variable: {e}. "
                "The DAG will be created but tasks may fail. Please configure them in the UI.")
    APPLICATION_ARGS = []


# ---------------------------------------------------------------------------
# นิยาม DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="vehicle_speed_analysis_production",
    description="วิเคราะห์ความเร็วของรถยนต์จากข้อมูล Event และแจ้งเตือนผ่าน Kafka",
    start_date=datetime(2025, 7, 7),
    schedule="*/10 * * * *",  # รันทุกๆ 5 นาที
    catchup=False,
    tags=["spark", "speed-analysis", "vehicle", "production"],
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=20),
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:

    check_cluster = PythonOperator(
        task_id="check_spark_cluster_health",
        python_callable=check_spark_cluster_health,
    )

    # ควรดึงค่า IP ของ pod ภายใน task เพื่อให้ได้ค่าล่าสุดเสมอ (สำหรับ Kubernetes)
    # หากรันบน môi trường อื่น อาจต้องใช้วิธีอื่นในการหา IP
    spark_driver_ip = os.environ.get("SPARK_DRIVER_POD_IP", "0.0.0.0")

    run_spark_job = SparkSubmitOperator(
        task_id="run_vehicle_speed_analysis_job",
        application=SCRIPT_PATH,
        conn_id=SPARK_CONN_ID,
        name="vehicle_speed_analysis_{{ ds_nodash }}_{{ ts_nodash }}",
        # total_executor_cores=10, 
        total_executor_cores=15,
        # --- การปรับแต่งประสิทธิภาพ Spark ---
        # ปรับค่าเหล่านี้ตามขนาดของ Cluster และข้อมูลของคุณ
        num_executors=1,
        executor_cores=4,
        executor_memory="8g",
        driver_memory="2g",
        
        # --- Spark configurations เพิ่มเติม ---
        conf={
            # การกำหนดพื้นฐาน
            "spark.driver.host": spark_driver_ip,
            "spark.driver.cores": "2",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.dynamicAllocation.enabled": "false",
            "spark.sql.adaptive.enabled": "false",
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

    check_cluster >> run_spark_job
