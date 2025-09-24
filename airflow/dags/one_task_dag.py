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
    dag_id='one_task_dag',
    description='A simple DAG with one task writing to /lab/temp',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:

    task1 = BashOperator(
        task_id='one_task',
        bash_command='mkdir -p /lab/temp && echo "This is a simple DAG with one task!" > /lab/temp/output.txt'
    )
