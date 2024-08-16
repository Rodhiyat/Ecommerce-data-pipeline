from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import pandas as pd
from io import StringIO
import os
from datetime import timedelta, datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Constants
PG_CONN_ID = os.getenv('PG_CONN_ID')
BQ_CONN_ID = os.getenv('BQ_CONN_ID')
BQ_PROJECT = os.getenv('BQ_PROJECT')
BQ_DATASET = os.getenv('BQ_DATASET')
BQ_BUCKET = os.getenv('BQ_BUCKET')
PG_SCHEMA = os.getenv('PG_SCHEMA')
PG_TABLES = os.getenv('PG_TABLES',' ').split(',')
BQ_TABLES = os.getenv('BQ_TABLES',' ').split(',')
CSV_FILENAMES = os.getenv('CSV_FILENAMES',' ').split(',')

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 13),   
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define schema for each table
schemas = {
    'customers': [
        {'name': 'customer_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'customer_unique_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'customer_zip_code_prefix', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'customer_city', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'customer_state', 'type': 'STRING', 'mode': 'NULLABLE'}
    ],
    'geolocation': [
        {'name': 'geolocation_zip_code_prefix', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'geolocation_lat', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'geolocation_lng', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'geolocation_city', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'geolocation_state', 'type': 'STRING', 'mode': 'NULLABLE'}
    ],
    'orders': [
        {'name': 'order_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'customer_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'order_status', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'order_purchase_timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'order_approved_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'order_delivered_carrier_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'order_delivered_customer_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'order_estimated_delivery_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
    ],
    'order_payments': [
        {'name': 'order_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'payment_sequential', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'payment_type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'payment_installments', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'payment_value', 'type': 'FLOAT', 'mode': 'NULLABLE'}
    ],
    'order_reviews': [
        {'name': 'review_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'order_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'review_score', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'review_comment_title', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'review_comment_message', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'review_creation_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'review_answer_timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
    ],
    'products': [
        {'name': 'product_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'product_category_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'product_name_length', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'product_description_length', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'product_photos_qty', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'product_weight_g', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'product_length_cm', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'product_height_cm', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'product_width_cm', 'type': 'INTEGER', 'mode': 'NULLABLE'}
    ],
    'sellers': [
        {'name': 'seller_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'seller_zip_code_prefix', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'seller_city', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'seller_state', 'type': 'STRING', 'mode': 'NULLABLE'}
    ],
    'order_items': [
        {'name': 'order_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'order_item_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'product_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'seller_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'shipping_limit_date', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
        {'name': 'price', 'type': 'FLOAT', 'mode': 'REQUIRED'},
        {'name': 'freight_value', 'type': 'FLOAT', 'mode': 'REQUIRED'}
    ]
}

def preprocess_csv(input_path: str, output_path: str, columns_to_keep: list):
    # Initialize Google Cloud Storage hook
    hook = GCSHook(gcp_conn_id=BQ_CONN_ID)
    
    # Extract bucket name and object path from input_path
    if not input_path.startswith('gs://'):
        raise ValueError(f"Invalid input path format: {input_path}")
    
    object_name = input_path[len('gs://'):]  # Remove 'gs://' prefix
    bucket_name, object_path = object_name.split('/', 1)
    
    # Download the CSV file from GCS
    csv_data = hook.download(bucket_name=bucket_name, object_name=object_path)
    
    # Load CSV data into a DataFrame
    df = pd.read_csv(StringIO(csv_data.decode('utf-8')))
    
    # Select only the columns of interest
    df = df[columns_to_keep]
    
    # Save the cleaned DataFrame to a new CSV file
    cleaned_csv = df.to_csv(index=False)
        
    # Upload the cleaned CSV to GCS
    hook.upload(bucket_name=bucket_name, object_name=output_path, data=cleaned_csv.encode('utf-8'),mime_type='text/csv')


# Initialize the DAG
dag = DAG(
    'postgres_to_gbq_etl',
    default_args=default_args,
    description='An Airflow DAG to extract data from PostgreSQL and load into BigQuery',
    schedule_interval=None,
    catchup=False,
)

# Dynamically create tasks for each table
for pg_table, bq_table, csv_filename in zip(PG_TABLES, BQ_TABLES, CSV_FILENAMES):
    postgres_data_to_gcs = PostgresToGCSOperator(
        task_id=f"postgres_data_to_gcs_{pg_table}",  # Unique task_id
        sql=f'SELECT * FROM "{PG_SCHEMA}"."{pg_table}";',
        bucket=BQ_BUCKET,
        filename=csv_filename,
        export_format='csv',
        postgres_conn_id=PG_CONN_ID,
        field_delimiter=',',
        gzip=False,
        gcp_conn_id=BQ_CONN_ID,
        dag=dag,
    )

    if pg_table == 'order_reviews':
        # Define the cleaned CSV filename
        cleaned_csv_filename = 'olist_order_reviews_dataset_cleaned.csv'
        
        # Task to preprocess the CSV file only for order_reviews
        preprocess_task = PythonOperator(
            task_id=f"preprocess_{pg_table}",
            python_callable=preprocess_csv,
            op_args=[
                "gs://etl-basics-testt/olist_order_reviews_dataset.csv",
                "olist_order_reviews_dataset_cleaned.csv",
                ['review_id', 'order_id', 'review_score', 'review_creation_date', 'review_answer_timestamp']
            ],
            dag=dag,
        )
        
        # Task to load cleaned data from GCS to BigQuery
        bq_ecommerce_load_csv = GCSToBigQueryOperator(
            task_id=f"bq_ecommerce_load_csv_{bq_table}",
            bucket=BQ_BUCKET,
            source_objects=[cleaned_csv_filename],
            destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.{bq_table}",
            schema_fields=[
                {"name": "review_id", "type": "STRING", "mode": "REQUIRED"},
                {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
                {"name": "review_score", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "review_creation_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
                {"name": "review_answer_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"}
                ],
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            gcp_conn_id=BQ_CONN_ID,
            autodetect=True,
            dag=dag,
        )
            
        # Set up the task dependencies for order_reviews
        postgres_data_to_gcs >> preprocess_task >> bq_ecommerce_load_csv

    else:
        # Task to load data directly from GCS to BigQuery for other tables
        bq_ecommerce_load_csv = GCSToBigQueryOperator(
            task_id=f"bq_ecommerce_load_csv_{bq_table}",  # Unique task_id
            bucket=BQ_BUCKET,
            source_objects=[csv_filename],
            destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.{bq_table}",
            schema_fields=schemas.get(bq_table, []),
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',  # Ensures idempotency
            gcp_conn_id=BQ_CONN_ID,
            autodetect=True,
            dag=dag,
        )

        postgres_data_to_gcs >> bq_ecommerce_load_csv