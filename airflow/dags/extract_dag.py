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
    dag_id='extract_dag',
    description='A simple DAG to extract data and write to /lab/orchestrated',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:

    task = BashOperator(
        task_id='extract_task',
        bash_command='wget -c https://raw.githubusercontent.com/LinkedInLearning/hands-on-introduction-data-engineering-4395021/main/data/top-level-domain-names.csv -O /lab/orchestrated/top-level-domain-names.csv'
    )
