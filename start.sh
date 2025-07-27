#!/bin/bash
set -e
echo "Starting docker..."
docker-compose up -d

echo "Waiting for airflow-worker to be ready..."
until docker-compose ps | grep airflow-worker | grep -q "Up"; do
  sleep 2
  echo "Still waiting..."
done

sleep 3

echo "Run Airflow DB init..."
#docker-compose exec airflow-worker airflow db migrate
docker-compose exec airflow-worker airflow db migrate || {
  echo "Airflow DB migration failed"
  docker-compose logs airflow-worker
  exit 1
}

echo "Upgrade Airflow DB schema..."
#docker-compose exec airflow-worker airflow db upgrade
docker-compose exec airflow-worker airflow db upgrade || {
  echo "Airflow DB upgrade failed"
  docker-compose logs airflow-worker
  exit 1
}

echo "Creating Airflow admin user..."
docker-compose exec airflow-worker airflow users create \
  --username airflow \
  --firstname Airflow \
  --lastname Admin \
  --role Admin \
  --email airflow@airflow.com \
  --password airflow || true

echo "Run SQL migration..."
docker-compose exec airflow-worker python3 /opt/airflow/migration.py

echo "List available dags..."
docker-compose exec airflow-worker airflow dags list

sleep 10

echo "You can now visit the Airflow UI at http://localhost:8080"
