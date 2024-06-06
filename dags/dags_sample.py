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


# Définiton du dag
dag = DAG(
    'example_postgres_dag',  # nom du dag
    # date de départ (pour schedule_intervale)
    start_date=datetime(2024, 6, 6),
    schedule_interval='@daily')  # définition de l'exécution automatique (ici tous les jours) -> en fonction de la date qu'on met dans start_date airflow va rattraper auto toutes les exécutions pas faites depuis la date

fetch_data_task = PythonOperator(  # première task du dag
    task_id='fetch_data_task',  # id de la task
    python_callable=fetch_data,
    dag=dag
)

fetch_data_task
