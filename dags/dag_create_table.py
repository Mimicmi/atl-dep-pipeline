from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
from airflow.models import Variable


def create_table():
    postgres_hook = PostgresHook(postgres_conn_id="POSTGRES_CONNEXION")
    
    create_table_queries = [
        """
        CREATE TABLE IF NOT EXISTS holidays_1 (
            date datetime,
            vacances_zone_a BOOLEAN NOT NULL,
            vacances_zone_b BOOLEAN NOT NULL,
            vacances_zone_c BOOLEAN NOT NULL,
            nom_vacances string NOT NULL,
            is_public_holiday BOOLEAN NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS temperatures_1 (
            timestamp datetime,
            trl FLOAT,
            tnl FLOAT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS profil_coefficients_1 (
            timestamp datetime,
            sous_profil string NOT NULL,
            cp FLOAT
        );
        """
    ]

    for query in create_table_queries:
        postgres_hook.run(query)
        print(f"Executed query: {query}")

# Définiton du dag
dag = DAG(
    'create_tables',  # nom du dag
    # date de départ (pour schedule_intervale)
    start_date=datetime(2024, 6, 6),
    schedule_interval='@daily')  # définition de l'exécution automatique (ici tous les jours)
# En fonction de la date qu'on met dans start_date airflow va rattraper auto toutes les exécutions pas faites depuis la date

create_table = PythonOperator(  # première task du dag
    task_id='create_table',  # id de la task
    python_callable=create_table,
    dag=dag
)

create_table