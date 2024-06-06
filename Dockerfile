FROM apache/airflow:2.7.2
COPY requirements_airflow.txt .
RUN pip install -r requirements_airflow.txt