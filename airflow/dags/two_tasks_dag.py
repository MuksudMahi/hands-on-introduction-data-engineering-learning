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
    dag_id='two_tasks_dag',
    description='A simple DAG with two tasks',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:

    t0 = BashOperator(
        task_id='bash_task_0',
        bash_command='echo "This is the first task in the DAG!"'
    )

    t1 = BashOperator(
        task_id='bash_task_1',
        bash_command='echo "This is the second task in the DAG!"'
    )

    t0 >> t1 