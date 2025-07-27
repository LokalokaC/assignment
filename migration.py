import logging
from pathlib import Path
from airflow.providers.postgres.hooks.postgres import PostgresHook

SQL_DIR = Path(__file__).resolve().parent / 'sql'

DB_CONFIG = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": 5432,
}

def run_migration(postgres_conn_id: str = 'postgres_default'):
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    logging.info("Running migration SQL...")
    sql_files = [
        SQL_DIR / 'orders.sql',
        SQL_DIR / 'customer_activities.sql'
    ]

    for sql_file in sql_files:
        with open(sql_file, 'r') as f:
            sql = f.read()
            logging.info(f"Executing full SQL from: {sql_file.name}")
            for query in sql.split(';'):
                q = query.strip()
                if q:
                    logging.info(f"Executing statement:\n{q}")
                    hook.run(q)
    logging.info("Migration completed successfully.")

if __name__ == "__main__":
    run_migration()