import logging
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from sqlalchemy.exc import SQLAlchemyError
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook

def load_df_to_postgres(output_path: str, table_name: str, postgres_conn_id: str, schema: str):
    """
    Load a Pandas DataFrame into a PostgreSQL table.
    Args:
        df (pd.DataFrame): The DataFrame to load.
        table_name (str): The name of the target PostgreSQL table.
        postgres_conn_id (str): The Airflow PostgreSQL connection ID.
    """
    logging.info(f"Attempting to load data into PostgreSQL table: {table_name}")
    
    path = Path(output_path)
    
    if not path.exists():
        raise FileNotFoundError(f"Config file {path} doesn't exist. Please check environments or paths")

    df = pd.read_parquet(path)

    if df.empty:
        logging.warning(f"DataFrame for table '{table_name}' is empty. Skipping load operation.")
        raise AirflowSkipException(f"Skipped loading: '{table_name}' has empty DataFrame.")
    
    df['_created_at'] = datetime.now(timezone.utc)

    try:
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        engine = pg_hook.get_sqlalchemy_engine()

        logging.info(f"Inserting {len(df)} rows into {table_name}...")
        df.to_sql(
                name=table_name, 
                con=engine, 
                if_exists='append',
                index=False, 
                schema=schema,
                method='multi',
                chunksize=500
            )
        logging.info(f"Successfully loaded {len(df)} rows into '{table_name}'.")

    except SQLAlchemyError as sa_e:
        logging.exception(f"[SQLAlchemyError] Failed to load data into table {table_name}: {sa_e}")
        raise
    except Exception as e:
        logging.exception(f"[General Error] Failed to load data into table {table_name}: {e}")
        raise