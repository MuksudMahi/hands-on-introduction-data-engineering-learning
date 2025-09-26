from datetime import datetime, date
from airflow import DAG
from airflow.operators.bash import BashOperator
import pandas as pd
from airflow.operators.python import PythonOperator

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
    dag_id='challenge_dag',
    description='A simple DAG to perform basic ETL operations',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:

    extract = BashOperator(
        task_id='extract_task',
        bash_command='wget -c https://raw.githubusercontent.com/LinkedInLearning/hands-on-introduction-data-engineering-4395021/main/data/constituents.csv -O /lab/challenge/constituents.csv'
    )

    def tranform_data():
        today = date.today() 
        df = pd.read_csv('/lab/challenge/constituents.csv')
        count_df = df.groupby('Sector').size().reset_index(name='Count')
        count_df.to_csv('/lab/challenge/constituents-transformed.csv', index=False)

    transform = PythonOperator(
        task_id='transform_task',
        python_callable=tranform_data,
        dag=dag
    )

    load = BashOperator(
        task_id='load_task',
        bash_command= 'echo -e ".separator ","\n.import --skip 1 /lab/challenge/constituents-transformed.csv sp_500_sector_count" | sqlite3 /lab/challenge/sp_500_sector_count.db',
        dag=dag
    )

    extract >> transform >> load

