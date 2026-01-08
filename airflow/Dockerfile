FROM apache/airflow:2.8.1-python3.10

USER root
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

USER airflow
COPY dags/ /opt/airflow/dags
COPY src/ /opt/airflow/src
