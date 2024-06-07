from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
from airflow.models import Variable
import logging
import pandas as pd
from vacances_scolaires_france import SchoolHolidayDates
import requests

# # Configurer le logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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
            day_of_week INTEGER,
            day_of_year INTEGER,
            half_hour INTEGER,
            fr_holiday VARCHAR(3),
            is_public_holiday boolean
        );
        """
    ]

    for query in create_table_queries:
        postgres_hook.run(query)
        print(f"Executed query: {query}")
        send_log("table crée")

    send_log("Fin de la création des tables")


def table_models_input():
    postgres_hook = PostgresHook(postgres_conn_id="POSTGRES_CONNEXION")

    send_log("Début création de la table models_inputs")
    create_table_queries = [
        """
        CREATE TABLE IF NOT EXISTS data_model_inputs_1 (
            timestamp TIMESTAMP,
            trl FLOAT,
            tnl FLOAT,
            sous_profil VARCHAR(255) ,
            cp FLOAT,
            day_of_week INTEGER,
            day_of_year INTEGER,
            half_hour INTEGER,
            fr_holiday VARCHAR(3),
            is_public_holiday boolean
        );
        """
    ]

    for query in create_table_queries:
        postgres_hook.run(query)
        print(f"Executed query: {query}")
        send_log("table crée")
    send_log("Fin de la création des tables")


def insert_holiday():
    postgres_hook = PostgresHook(postgres_conn_id="POSTGRES_CONNEXION")

    d = SchoolHolidayDates()

    france_holidays_2023 = d.holidays_for_year(2023)
    send_log("Récupération des données API vacances ok")

    # Convertir le dictionnaire en liste de dictionnaires
    list_holidays = [value for key, value in france_holidays_2023.items()]

    # Créer le DataFrame
    send_log("Création du Dataframe vacances")
    df_holidays = pd.DataFrame(list_holidays)

    # Ajouter une colonne 'is_public_holiday' avec des valeurs par défaut (par exemple, False)
    df_holidays['is_public_holiday'] = False
    send_log("Dataframe ready")

    # Connexion à la base de données et exécution de la requête
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    send_log("Debut insertion des données de vacances")
    for index, row in df_holidays.iterrows():
        cursor.execute("INSERT INTO holidays_1 (date, vacances_zone_a, vacances_zone_b, vacances_zone_c, nom_vacances, is_public_holiday) VALUES (%s,%s,%s,%s,%s,%s)",
                       (row['date'], row['vacances_zone_a'], row['vacances_zone_b'], row['vacances_zone_c'], row['nom_vacances'], row['is_public_holiday']))

    conn.commit()
    cursor.close()
    conn.close()


def insert_temperature():
    postgres_hook = PostgresHook(postgres_conn_id="POSTGRES_CONNEXION")

    response_temp = requests.get(
        "https://data.enedis.fr//api/explore/v2.1/catalog/datasets/donnees-de-temperature-et-de-pseudo-rayonnement/records")

    response_temp = response_temp.json()
    send_log("Récupération des données API temperature ok")

    records_temp = response_temp["results"]

    timestamps_temp = []
    trl = []
    tnl = []

    for record in records_temp:
        timestamps_temp.append(record["horodate"])
        trl.append(record["temperature_realisee_lissee_degc"])
        tnl.append(record["temperature_normale_lissee_degc"])

    send_log("Création du Dataframe temperature")
    df_temp = pd.DataFrame({
        "timestamp": pd.to_datetime(timestamps_temp),
        "trl": pd.to_numeric(trl),
        "tnl": pd.to_numeric(tnl)
    })

    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    send_log("Debut insertion des données de températures")
    for index, row in df_temp.iterrows():
        cursor.execute("INSERT INTO temperatures_1 (timestamp, trl, tnl) VALUES (%s,%s,%s)",
                       (row['timestamp'], row['trl'], row['tnl']))

    conn.commit()
    cursor.close()
    conn.close()


def insert_coefficient_profile():
    postgres_hook = PostgresHook(postgres_conn_id="POSTGRES_CONNEXION")

    response_profil = requests.get(
        "https://data.enedis.fr/api/explore/v2.1/catalog/datasets/coefficients-des-profils/records")

    response_profil = response_profil.json()
    records_profil = response_profil["results"]
    send_log("Récupération des données API coefficient profil ok")

    timestamps_profil = []
    sous_profil = []
    cp = []

    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    for record in records_profil:
        timestamps_profil.append(record["horodate"])
        sous_profil.append(record["sous_profil"])
        cp.append(record["coefficient_ajuste"])

    df_profil = pd.DataFrame({
        "timestamp": pd.to_datetime(timestamps_profil),
        "sous_profil": sous_profil,
        "cp": pd.to_numeric(cp)
    })

    df_profil["sous_profil"] = df_profil["sous_profil"].astype(str)

    send_log("Debut insertion des données de coefficient profil")
    for index, row in df_profil.iterrows():
        cursor.execute("INSERT INTO profil_coefficients_1 (timestamp, sous_profil, cp) VALUES (%s,%s,%s)",
                       (row['timestamp'], row['sous_profil'], row['cp']))

    conn.commit()
    cursor.close()
    conn.close()


def dag_success_alert(context):
    print(f"DAG has succeeded, run_id: {context['run_id']}")


def dag_failure_alert(context):
    print(f"Dag has failed its execution, run_id: {context['run_id']}")


dag = DAG(
    'creation_bdd',
    start_date=datetime(2024, 6, 6),
    schedule_interval='@daily',
    on_success_callback=dag_success_alert,
    on_failure_callback=dag_failure_alert
)

tables_base = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag
)

holiday = PythonOperator(
    task_id='holiday',
    python_callable=insert_holiday,
    dag=dag
)

temperature = PythonOperator(
    task_id='temperature',
    python_callable=insert_temperature,
    dag=dag
)

table_models = PythonOperator(
    task_id='table_models',
    python_callable=table_models_input,
    dag=dag
)

coefficient_profil = PythonOperator(
    task_id='coefficient_profil',
    python_callable=insert_coefficient_profile,
    dag=dag
)

tables_base >> [holiday, temperature, coefficient_profil] >> table_models
