from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# The SQL command for testing.
# This query selects a specific record from the public.score_event table.
SQL_TEST_COMMAND = "SELECT * FROM public.score_event WHERE pk_id='381f69e0-5a3a-11f0-9744-5693fdde0af4';"

# Define the DAG
with DAG(
    dag_id="postgres_test_query_dag",
    start_date=pendulum.datetime(2025, 7, 6, tz="Asia/Bangkok"),
    schedule=" 0 0 * * *",  # Runs daily at midnight
    catchup=False,
    doc_md="""
    ### PostgreSQL Test Query DAG
    
    This DAG connects to a PostgreSQL database and executes a test SELECT query
    on the `public.score_event` table.
    Make sure your `postgres_lor_db` connection is configured correctly in the Airflow UI.
    """,
    tags=["postgres", "test"],
) as dag:
    
    # Define the task using the SQLExecuteQueryOperator
    run_test_query = SQLExecuteQueryOperator(
        task_id="test_select_from_score_event",
        # This is the Connection ID from your Airflow UI.
        conn_id="postgres_lor_db",
        sql=SQL_TEST_COMMAND,
    )