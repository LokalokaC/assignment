FROM apache/airflow:2.8.1-python3.11
ENV AIRFLOW_HOME=/opt/airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR ${AIRFLOW_HOME}

COPY dags/ ${AIRFLOW_HOME}/dags/
COPY src/ ${AIRFLOW_HOME}/src/
COPY . ${AIRFLOW_HOME}/.