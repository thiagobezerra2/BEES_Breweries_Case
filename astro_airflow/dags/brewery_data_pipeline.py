from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024,4,15, 7,0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'brewery_data_pipeline',
    default_args=default_args,
    description='ETL pipeline for Brewery Data',
    schedule_interval=timedelta(days=1),
)

extract_task = BashOperator(
    task_id='extract_and_trasnform',
    bash_command='pyton /opt/astro_airflow/dags/extraction_api.py',
    dag=dag,
)

transform_task = BashOperator(
    task_id='transform',
    bash_command='pyton /opt/astro_airflow/dags/transform_data.py',
    dag=dag,
)
aggregate_task = BashOperator(
    task_id='load',
    bash_command='pyton /opt/astro_airflow/dags/aggdata.py',
    dag=dag,
)




#Orchestration
extract_task >> transform_task >> aggregate_task
