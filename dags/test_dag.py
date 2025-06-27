from __future__ import annotations
import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator  # ← use EmptyOperator

with DAG(
    dag_id="simple_etl_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["etl", "example"],
    doc_md="""
    # Simple ETL DAG
    This DAG demonstrates a basic Extract, Transform, Load (ETL) process.
    - **Extract**: Simulates pulling data.
    - **Transform**: Simulates processing data.
    - **Load**: Simulates pushing data to a destination.
    """,
) as dag:

    start_task = EmptyOperator(task_id="start")

    extract_task = BashOperator(
        task_id="extract_data",
        bash_command="echo 'Simulating data extraction...'; sleep 2",
    )

    transform_task = BashOperator(
        task_id="transform_data",
        bash_command="echo 'Simulating data transformation...'; sleep 3",
    )

    load_task = BashOperator(
        task_id="load_data",
        bash_command="echo 'Simulating data loading...'; sleep 2",
    )

    end_task = EmptyOperator(task_id="end")

    start_task >> extract_task >> transform_task >> load_task >> end_task
