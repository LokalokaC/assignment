from datetime import datetime, timedelta
import logging
from airflow.decorators import dag, task

@dag(
    dag_id='order_data_processing_taskflow',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    is_paused_upon_creation=False,
    tags=['orders', 'data_pipeline'],
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
)
def order_data_processing_taskflow(
    FILE_NAMES: list = ['order_data_1.csv','order_data_2.csv'],
    TARGET_TABLE_NAME: str = 'orders',
    SCHEMA: str = 'business',
    POSTGRES_CONN_ID: str = 'postgres_default'
):
    @task()
    def extract_data_task(file_names: list[str]) -> list[str]:
        from src.extract import extract_data
        return extract_data(file_names)

    @task()
    def transform_task(input_paths: list) -> str:
        from src.transform import transform_and_merge

        output_path = transform_and_merge(input_paths)

        logging.info(f"Saved transformed DataFrame to {output_path}")
        return output_path

    @task()
    def load_task(output_path: str, table: str, conn_id: str, schema: str):
        from src.load import load_df_to_postgres
        logging.info(f"Loading dataframe into {table} using connection {conn_id}")

        load_df_to_postgres(output_path, table, conn_id, schema)
        
        logging.info("Loading completed successfully.")

    _order_parquet = extract_data_task(file_names=FILE_NAMES)
    _merged_parquet = transform_task(_order_parquet)
    load_task(
         output_path=_merged_parquet,
         table=TARGET_TABLE_NAME,
         conn_id=POSTGRES_CONN_ID,
         schema=SCHEMA
         )
    
order_data_processing_taskflow()