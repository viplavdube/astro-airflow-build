from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from include.utils.message import print_message
from include.utils.data_ingestion import ingestion_process


with DAG(
    dag_id="sales_product_branch",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    schedule=None
) as dag:
    start_task = EmptyOperator(task_id="start")
    data_ingestion_task = PythonOperator(task_id="data_ingestion", python_callable=ingestion_process)
    end_task = EmptyOperator(task_id="end")

    start_task >> data_ingestion_task >> end_task