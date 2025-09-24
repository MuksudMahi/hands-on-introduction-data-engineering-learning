from datetime import datetime, date
from airflow import DAG
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
    dag_id='transform_dag',
    description='A simple DAG to transform data and write to /lab/orchestrated',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:
    
    def tranform_data():
        today = date.today()
        df = pd.read_csv('/lab/orchestrated/top-level-domain-names.csv')
        generic_type_df = df[df['Type'] == 'generic']
        generic_type_df['Date'] = today.strftime("%Y-%m-%d")
        generic_type_df.to_csv('/lab/orchestrated/generic-top-level-domain-names-transformed.csv', index=False)

    task = PythonOperator(
        task_id='transform_task',
        python_callable=tranform_data,
        dag=dag
    )
