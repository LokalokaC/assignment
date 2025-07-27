import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task
@dag(
    dag_id = 'customer_activities_processing_taskflow',
    start_date = datetime(2025,1,1),
    schedule_interval='@daily',
    catchup=False,
    is_paused_upon_creation=False,
    tags=['customer_logs','data_pipeline'],
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
)
def customer_data_processing_taskflow(
    FILE_NAME: list = ['customer.log'],
    TARGET_TABLE_NAME: str = 'customer_activities',
    SCHEMA: str = 'business',
    POSTGRES_CONN_ID: str = 'postgres_default'
):
    @task()
    def extract_data_task(file_name: list[str]) -> list[str]:
        from src.extract import extract_data
        return extract_data(file_name)

    @task()
    def transform_task(input_path: list[str])-> str:
        from src.transform import transform
       
        output_path = transform(input_path)

        logging.info(f"Saved transformed DataFrame to {output_path}")

        return output_path

    @task()
    def load_task(cleaned_parquet: str, table: str, conn_id: str, schema: str):
        from src.load import load_df_to_postgres
        logging.info(f"Loading dataframe into {table} using connection {conn_id}")

        load_df_to_postgres(cleaned_parquet, table, conn_id, schema)
        
        logging.info("Loading completed successfully.")

    _customer_parquet = extract_data_task(file_name=FILE_NAME)
    _cleaned_parquet = transform_task(input_path=_customer_parquet)
    load_task(
        cleaned_parquet=_cleaned_parquet,
        table=TARGET_TABLE_NAME,
        conn_id=POSTGRES_CONN_ID,
        schema=SCHEMA
        )
    
customer_data_processing_taskflow()