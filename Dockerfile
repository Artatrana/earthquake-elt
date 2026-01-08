# Start from official Airflow image
FROM apache/airflow:2.8.1-python3.10

# Install extra Python dependencies
USER root
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your DAGs and source code
USER airflow
COPY dags/ /opt/airflow/dags/
COPY src/ /opt/airflow/src/
FROM apache/airflow:2.8.1-python3.10

USER airflow

# Copy pyproject + source
COPY pyproject.toml README.md /opt/airflow/
COPY src /opt/airflow/src

# Install editable package
RUN pip install --no-cache-dir -e /opt/airflow