from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime


def fetch_data():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_connection')
    records = postgres_hook.get_records(
        sql='SELECT * FROM profil_coefficients_1 LIMIT 10')
    for record in records:
        print(record)


dag = DAG(
    'example_postgres_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily')

fetch_data_task = PythonOperator(
    task_id='fetch_data_task',
    python_callable=fetch_data,
    dag=dag
)

fetch_data_task
