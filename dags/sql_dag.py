#
# Author: Gemini
# Date: July 6, 2025
# Description: This Airflow DAG runs a daily maintenance task on a PostgreSQL
#              database using pg_partman.
#

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# The SQL command to be executed.
# This calls the run_maintenance function from the pg_partman extension
# to manage time-based or serial-based table partitions.
SQL_MAINTENANCE_COMMAND =  "SELECT * FROM public.score_event WHERE pk_id='381f69e0-5a3a-11f0-9744-5693fdde0af4';" #"SELECT partman.run_maintenance('public.vehicle_events');"

# Define the DAG
with DAG(
    dag_id="postgres_partman_maintenance_dag",
    start_date=pendulum.datetime(2025, 7, 6, tz="Asia/Bangkok"),
    schedule_interval="@daily",  # You can change this to your desired schedule, e.g., "0 1 * * *" for 1 AM daily
    catchup=False,
    doc_md="""
    ### PostgreSQL Partition Maintenance DAG

    This DAG connects to a PostgreSQL database and executes the `partman.run_maintenance` function.
    It is scheduled to run daily. Make sure your `postgres_lor_db` connection is configured
    correctly in the Airflow UI.
    """,
    tags=["postgres", "maintenance", "partman"],
) as dag:
    # Define the task using the PostgresOperator
    run_partition_maintenance = PostgresOperator(
        task_id="run_partman_maintenance_on_vehicle_events",
        # This is the Connection ID from your Airflow UI.
        # Based on your screenshot, it is 'postgres_lor_db'.
        postgres_conn_id="postgres_lor_db",
        sql=SQL_MAINTENANCE_COMMAND,
        # autocommit=True ensures that the transaction is committed.
        autocommit=True,
    )

