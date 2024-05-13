import sys
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines.normalize_pipeline import normalize_pipeline
from pipelines.denormalize_pipeline import denormalize_pipeline


default_args = {
    'owner': 'DimaKuriptya',
    'start_date': datetime(2024, 5, 13)
}

with DAG(
    dag_id='cars_normalize_denormalize',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['normalization', 'denormalization', 'etl', 'pipeline']
) as dag:
    normalize = PythonOperator(
        task_id='normalize_data',
        python_callable=normalize_pipeline
    )

    denormalize = PythonOperator(
        task_id='denormalize_data',
        python_callable=denormalize_pipeline
    )

    normalize >> denormalize
