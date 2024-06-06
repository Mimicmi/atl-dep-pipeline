from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
from airflow.models import Variable
import pandas as pd
from vacances_scolaires_france import SchoolHolidayDates
import requests


def insert_holiday():
    postgres_hook = PostgresHook(postgres_conn_id="POSTGRES_CONNEXION")

    d = SchoolHolidayDates()

    france_holidays_2023 = d.holidays_for_year(2023)

    # Convertir le dictionnaire en liste de dictionnaires
    list_holidays = [value for key, value in france_holidays_2023.items()]

    # Créer le DataFrame
    df_holidays = pd.DataFrame(list_holidays)

    # Ajouter une colonne 'is_public_holiday' avec des valeurs par défaut (par exemple, False)
    df_holidays['is_public_holiday'] = False

    # Connexion à la base de données et exécution de la requête
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

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

    records_temp = response_temp["results"]

    timestamps_temp = []
    trl = []
    tnl = []

    for record in records_temp:
        timestamps_temp.append(record["horodate"])
        trl.append(record["temperature_realisee_lissee_degc"])
        tnl.append(record["temperature_normale_lissee_degc"])

    df_temp = pd.DataFrame({
        "timestamp": pd.to_datetime(timestamps_temp),
        "trl": pd.to_numeric(trl),
        "tnl": pd.to_numeric(tnl)
    })

    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    for index, row in df_temp.iterrows():
        cursor.execute("INSERT INTO temperatures_1 (timestamp, trl, tnl) VALUES (%s,%s,%s)",
                       row['timestamp'], row['trl'], row['tnl'])

    conn.commit()
    cursor.close()
    conn.close()


def insert_profile():
    postgres_hook = PostgresHook(postgres_conn_id="POSTGRES_CONNEXION")

    response_profil = requests.get(
        "https://data.enedis.fr/api/explore/v2.1/catalog/datasets/coefficients-des-profils/records")

    response_profil = response_profil.json()
    records_profil = response_profil["results"]

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

    for index, row in df_profil.iterrows():
        cursor.execute("INSERT INTO profil_coefficients_1 (timestamp, sous_profil, cp) VALUES (%s,%s,%s)",
                       row['timestamp'], row['sous_profil'], row['cp'])

    conn.commit()
    cursor.close()
    conn.close()


dag = DAG(
    'holiday',
    start_date=datetime(2024, 6, 6),
    schedule_interval='@daily')

holiday = PythonOperator(
    task_id='holiday',
    python_callable=insert_holiday,
    dag=dag
)

holiday
