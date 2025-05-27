from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime
import subprocess

def run_script(script_name):
    subprocess.run(["python", script_name], check=True)

default_args = {
    'owner': 'Afsaruddin',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 1),
    'retries': 1,
}

with DAG(
    dag_id='state_analysis_etl',
    default_args=default_args,
    schedule_interval='0 0 * * *',  # Daily at midnight
    catchup=False,
    description='ETL pipeline for state housing and income analysis, runs when new Redfin file is available',
) as dag:

    wait_for_redfin = FileSensor(
        task_id='wait_for_redfin',
        filepath='data/raw/REDFIN_MEDIAN_SALE_PRICE.csv',
        poke_interval=300,  # Check every 5 minutes
        timeout=60*60*6,    # Timeout after 6 hours
        mode='poke'
    )

    extract = PythonOperator(
        task_id='extract',
        python_callable=lambda: run_script('etl/extract.py')
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=lambda: run_script('etl/transform.py')
    )

    load = PythonOperator(
        task_id='load',
        python_callable=lambda: run_script('etl/load.py')
    )

    wait_for_redfin >> extract >> transform >> load
