from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from include.utils.message import print_message


with DAG(
    dag_id="product_branch",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    schedule=None
) as dag:
    start_task = EmptyOperator(task_id="start")
    print_task = PythonOperator(task_id="print_task", python_callable=print_message)
    end_task = EmptyOperator(task_id="end")

    start_task >> print_task >> end_task