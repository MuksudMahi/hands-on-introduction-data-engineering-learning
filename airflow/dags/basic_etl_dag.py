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
    dag_id='basic_etl_dag',
    description='A simple DAG to perform basic ETL operations',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:

    extract = BashOperator(
        task_id='extract_task',
        bash_command='wget -c https://raw.githubusercontent.com/LinkedInLearning/hands-on-introduction-data-engineering-4395021/main/data/top-level-domain-names.csv -O /lab/end-to-end/top-level-domain-names.csv'
    )

    def tranform_data():
        today = date.today()
        df = pd.read_csv('/lab/end-to-end/top-level-domain-names.csv')
        generic_type_df = df[df['Type'] == 'generic']
        generic_type_df['Date'] = today.strftime("%Y-%m-%d")
        generic_type_df.to_csv('/lab/end-to-end/generic-top-level-domain-names-transformed.csv', index=False)

    transform = PythonOperator(
        task_id='transform_task',
        python_callable=tranform_data,
        dag=dag
    )

    load = BashOperator(
        task_id='load_task',
        bash_command= 'echo -e ".separator ","\n.import --skip 1 /lab/end-to-end/generic-top-level-domain-names-transformed.csv top_level_domains" | sqlite3 /lab/end-to-end/top_level_domains.db',
        dag=dag
    )

    extract >> transform >> load

