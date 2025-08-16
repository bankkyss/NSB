from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# The SQL commands for running pg_partman maintenance on different tables.
SQL_MAINTENANCE_VEHICLE_EVENTS = "SELECT partman.run_maintenance('public.vehicle_events');"
SQL_MAINTENANCE_RAW_EVENT_DATA = "SELECT partman.run_maintenance('public.raw_event_data');"
SQL_MAINTENANCE_SCORE_EVENT = "SELECT partman.run_maintenance('public.score_event');"
SQL_MAINTENANCE_LOG_EVENT = "SELECT partman.run_maintenance('public.log_entity');"
# Define the DAG
with DAG(
    dag_id="postgres_partition_maintenance_dag",
    start_date=pendulum.datetime(2025, 7, 6, tz="Asia/Bangkok"),
    schedule="0 */12 * * *", # Runs every 12 hours
    catchup=False,
    doc_md="""
    ### PostgreSQL Partition Maintenance DAG

    This DAG runs `partman.run_maintenance` for several partitioned tables
    in a PostgreSQL database.

    - **Connection ID:** `postgres_lpr_db`
    - **Tables Maintained:**
        - `public.vehicle_events`
        - `public.raw_event_data`
        - `public.score_event`
    
    Ensure your `postgres_lpr_db` connection is configured correctly in the Airflow UI.
    """,
    tags=["postgres", "maintenance", "partition", "partman"],
) as dag:
    
    # Task to run maintenance on the vehicle_events table
    run_maintenance_vehicle_events = SQLExecuteQueryOperator(
        task_id="run_maintenance_on_vehicle_events",
        # This is the Connection ID from your Airflow UI.
        conn_id="postgres_lpr_db",
        sql=SQL_MAINTENANCE_VEHICLE_EVENTS,
    )

    # Task to run maintenance on the raw_event_data table
    run_maintenance_raw_event_data = SQLExecuteQueryOperator(
        task_id="run_maintenance_on_raw_event_data",
        conn_id="postgres_lpr_db",
        sql=SQL_MAINTENANCE_RAW_EVENT_DATA,
    )

    # Task to run maintenance on the score_event table
    run_maintenance_score_event = SQLExecuteQueryOperator(
        task_id="run_maintenance_on_score_event",
        conn_id="postgres_lpr_db",
        sql=SQL_MAINTENANCE_SCORE_EVENT,
    )
    run_maintenance_log_event = SQLExecuteQueryOperator(
        task_id="run_maintenance_on_log_event",
        conn_id="postgres_log_db",
        sql=SQL_MAINTENANCE_LOG_EVENT,
    )
    # Define the task dependency chain.
    # The tasks will run sequentially in this order.
    run_maintenance_vehicle_events >> run_maintenance_raw_event_data >> run_maintenance_score_event >> run_maintenance_log_event
