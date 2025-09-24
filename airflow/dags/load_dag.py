from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Muksud',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'catchup': False,
    'start_date': datetime(2025, 1, 1),
}

with DAG(
    dag_id='load_dag',
    description='A simple to load data to database',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:

    task1 = BashOperator(
        task_id='load_task',
        bash_command= 'echo -e ".separator ","\n.import --skip 1 /lab/orchestrated/generic-top-level-domain-names-transformed.csv top_level_domains" | sqlite3 /lab/orchestrated/top_level_domains.db',
        dag=dag
    )
