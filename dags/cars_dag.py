from airflow import DAG
from datetime import datetime
import pathlib
import sys
import os
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from normalize import normalize_pipeline


default_args = {
    'owner': 'DimaKuriptya',
    'start_date': datetime(2024, 5, 8)
}


with DAG(
    dag_id='cars_normalization',
    default_args=default_args,
    catchup=False,
    tags=['normalization', 'denormalization', 'etl']
) as dag:
    normalize = PythonOperator(
        task_id='normalize_data',
        python_callable=normalize_pipeline
    )
