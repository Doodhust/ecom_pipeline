from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import subprocess


def generate_sales():
       subprocess.call(['python', 'app/generate_sales.py'])

def load_postgres():
       subprocess.call(['python', 'app/load_postgres.py'])
    
def transfer_to_clickhouse():
       subprocess.call(['python', 'app/transfer_to_clickhouse.py'])


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(20),
}

dag = DAG(
    'sales_data_processing',
    default_args=default_args,
    description='DAG для обработки данных о продажах',
    schedule='45 12 * * 2',
    catchup=True,
    # timezone='Europe/Moscow'
)

t1 = PythonOperator(
    task_id='generate_sales',
    python_callable=generate_sales,
    dag=dag,
)

t2 = PythonOperator(
    task_id='load_postgres',
    python_callable=load_postgres,
    dag=dag,
)

t3 = PythonOperator(
    task_id='transfer_to_clickhouse',
    python_callable=transfer_to_clickhouse,
    dag=dag,
)

t1 >> t2 >> t3
