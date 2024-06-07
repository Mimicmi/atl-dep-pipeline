from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.models import Variable
import logging
import pandas as pd
from vacances_scolaires_france import SchoolHolidayDates
import requests
from airflow.utils.email import send_email
from email.message import EmailMessage
import smtplib
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator


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


def insert_transformed_data():
    postgres_hook = PostgresHook(postgres_conn_id="POSTGRES_CONNEXION")

    join_query = """
    WITH rounded_temps AS (
    SELECT
        timestamp,
        trl,
        tnl,
        DATE_TRUNC('minute', timestamp) AS rounded_timestamp
    FROM
        temperatures_1
    ),
    rounded_profil AS (
        SELECT
            timestamp,
            sous_profil,
            cp,
            DATE_TRUNC('minute', timestamp) AS rounded_timestamp
        FROM
            profil_coefficients_1
    )
    SELECT
        t.timestamp,
        t.trl,
        t.tnl,
        COALESCE(p.sous_profil, '') AS sous_profil,
        COALESCE(p.cp, 0) AS cp,
        EXTRACT(ISODOW FROM t.timestamp) - 1 AS day_of_week,
        EXTRACT(MONTH FROM t.timestamp) AS month,
        (EXTRACT(HOUR FROM t.timestamp) * 2 + EXTRACT(MINUTE FROM t.timestamp) / 30 + 1) AS half_hour,
        CONCAT(
            CASE WHEN h.vacances_zone_a THEN '1' ELSE '0' END,
            CASE WHEN h.vacances_zone_b THEN '1' ELSE '0' END,
            CASE WHEN h.vacances_zone_c THEN '1' ELSE '0' END
        ) AS fr_holiday,
        h.is_public_holiday
    FROM
        rounded_temps t
    LEFT JOIN
        rounded_profil p ON t.rounded_timestamp = p.rounded_timestamp AND t.timestamp = p.timestamp
    LEFT JOIN
        holidays_1 h ON t.timestamp::DATE = h.date::DATE;
    """

    # Insert data into data_model_inputs_1
    insert_query = f"""
    INSERT INTO data_model_inputs_1 (timestamp, trl, tnl, sous_profil, cp, day_of_week, day_of_year, half_hour, fr_holiday, is_public_holiday)
    {join_query};
    """

    postgres_hook.run(insert_query)
    print("Data inserted into data_model_inputs_1.")


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


def fetch_all_data(url):
    start_date = str((datetime.today() - timedelta(days=8)).date())
    end_date = str((datetime.today() - timedelta(days=1)).date())
    base_url = f"{url}?where=horodate%20%3E%20'{start_date}'%20and%20horodate%20%3C%20'{end_date}'"

    all_data = []
    offset = 0
    limit = 100  # Adjust as per API's limit parameter

    while True:
        response = requests.get(
            base_url, params={"limit": limit, "offset": offset})
        data = response.json()
        print(data)
        records = data["results"]
        all_data.extend(records)

        if len(records) < limit:
            break

        offset += limit

    return pd.DataFrame(all_data)


def insert_temperature(**kwargs):
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)

    # Connexion à la base de données
    hook = PostgresHook(postgres_conn_id="POSTGRES_CONNEXION")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Vérifier si la table existe, sinon la créer
    cursor.execute("""
         CREATE TABLE IF NOT EXISTS temperatures_1 (
            timestamp TIMESTAMP,
            trl FLOAT,
            tnl FLOAT
        );
    """)
    conn.commit()

    # Vérifier si la table est vide
    cursor.execute("SELECT COUNT(*) FROM temperatures_1;")
    table_count = cursor.fetchone()[0]

    df = fetch_all_data(
        'https://data.enedis.fr//api/explore/v2.1/catalog/datasets/donnees-de-temperature-et-de-pseudo-rayonnement/records')

    # logging.info(type(df['timestamp']))

    if table_count == 0:  # Si la table est vide, récupérer toutes les données depuis 01-01-2023
        for index, row in df.iterrows():
            cursor.execute(""" INSERT INTO temperatures_1 (timestamp, trl, tnl) VALUES (%s, %s,%s)""",
                           (row['horodate'], row['temperature_realisee_lissee_degc'], row['temperature_normale_lissee_degc']))

    else:  # Si la table n'est pas vide, récupérer le dernier timestamp
        cursor.execute("""
            SELECT MAX(timestamp) FROM temperatures_1;
        """)
        last_timestamp = cursor.fetchone()[0]
        if last_timestamp < (datetime.today() - timedelta(days=8)).date():
            for index, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO temperatures (timestamp, trl, tnl)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (timestamp)
                    DO UPDATE SET
                        trl = EXCLUDED.trl,
                        tnl = EXCLUDED.tnl;""", (row['horodate'], row['temperature_realisee_lissee_degc'], row['temperature_normale_lissee_degc']))
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


def check_condition(**kwargs):
    # Simulate user input or some condition
    user_input = kwargs['dag_run'].conf.get('effacer_bdd')
    send_log(user_input)
    if user_input.lower() == "true":
        return "effacer_bdd"
    else:
        return "false_branch"


def task_to_run():
    postgres_hook = PostgresHook(postgres_conn_id="POSTGRES_CONNEXION")

    send_log("Début de suppression des tables")

    delete_table_queries = [
        """
        TRUNCATE TABLE  holidays_1;
        """,
        """
        TRUNCATE TABLE  temperatures_1;
        """,
        """
        TRUNCATE TABLE  profil_coefficients_1;
        """,
        """
        TRUNCATE TABLE  data_model_inputs_1;
        """,
        """
        DROP TABLE  holidays_1;
        """,
        """
        DROP TABLE  temperatures_1;
        """,
        """
        DROP TABLE  profil_coefficients_1;
        """,
        """
        DROP TABLE  data_model_inputs_1;
        """
    ]

    for query in delete_table_queries:
        postgres_hook.run(query)
        print(f"Executed query: {query}")

    send_log("Fin de la suppression des tables")


@task(task_id="envoi_mail")
def send_mail(subject, content):
    # Configuration de l'envoi de mail
    gmail_cfg = {
        "server": "smtp.gmail.com",
        "port": "465",
        "email": "",  # TODO: METTEZ VOTRE ADRESSE EMAIL
        "pwd": ""  # TODO: METTEZ VOTRE MOT DE PASSE D'APPLICATION SI SMTP GMAIL
    }

    msg = EmailMessage()
    msg["to"] = gmail_cfg["email"]
    msg["from"] = gmail_cfg["email"]
    msg["Subject"] = subject
    msg.set_content(content)

    with smtplib.SMTP_SSL(gmail_cfg["server"], int(gmail_cfg["port"])) as smtp:
        smtp.login(gmail_cfg["email"], gmail_cfg["pwd"])
        smtp.send_message(msg)


send_succes_mail = send_mail("Airflow DAG", "Execution avec succès")

dag = DAG(
    'creation_bdd',
    start_date=datetime(2024, 6, 6),
    schedule_interval='0 8 * * *',
    params={"effacer_bdd": "false"},
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

insert_data_task = PythonOperator(
    task_id='insert_transformed_data',
    python_callable=insert_transformed_data,
    dag=dag,
)

check_condition_task = BranchPythonOperator(
    task_id='check_condition',
    python_callable=check_condition,
    dag=dag,
)

true_branch_task = PythonOperator(
    task_id='effacer_bdd',
    python_callable=task_to_run,
    dag=dag,
)

false_branch_task = DummyOperator(
    task_id='false_branch',
    dag=dag,
)

check_condition_task >> [true_branch_task, false_branch_task]
true_branch_task >> tables_base >> [
    holiday, temperature, coefficient_profil] >> insert_data_task >> table_models >> send_succes_mail
