from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
from airflow.models import Variable
import pandas as pd
from vacances_scolaires_france import SchoolHolidayDates


def holiday():
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


dag = DAG(
    'holiday',
    start_date=datetime(2024, 6, 6),
    schedule_interval='@daily')

holiday = PythonOperator(
    task_id='holiday',
    python_callable=holiday,
    dag=dag
)

holiday
