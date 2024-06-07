from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
from airflow.models import Variable
import logging

# # Configurer le logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# # Créer un gestionnaire de console et définir le niveau de log
# console_handler = logging.StreamHandler()
# console_handler.setLevel(logging.INFO)

# # Créer un formatteur et l'ajouter au gestionnaire
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# console_handler.setFormatter(formatter)

# # Ajouter le gestionnaire au logger
# logger.addHandler(console_handler)

def send_log(message):
    logger.info(message)

def create_table():
    postgres_hook = PostgresHook(postgres_conn_id="POSTGRES_CONNEXION")
    send_log("Début création des tables")
    create_table_queries = [
        """
        CREATE TABLE IF NOT EXISTS holidays_1 (
            date TIMESTAMP,
            vacances_zone_a BOOLEAN ,
            vacances_zone_b BOOLEAN ,
            vacances_zone_c BOOLEAN ,
            nom_vacances VARCHAR(255) ,
            is_public_holiday BOOLEAN 
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS temperatures_1 (
            timestamp TIMESTAMP,
            trl FLOAT,
            tnl FLOAT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS profil_coefficients_1 (
            timestamp TIMESTAMP,
            sous_profil VARCHAR(255) ,
            cp FLOAT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS data_model_inputs_1 (
            timestamp TIMESTAMP,
            trl FLOAT,
            tnl FLOAT,
            sous_profil VARCHAR(255) ,
            cp FLOAT,
            day_of_week integer(1),
            day_of_year integer(2),
            half_hour integer(2),
            fr_holiday integer(3),
            is_public_holiday boolean
        );
        """
    ]

    for query in create_table_queries:
        postgres_hook.run(query)
        print(f"Executed query: {query}")
        send_log("table crée")
    send_log("Fin de la création des tables")

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
