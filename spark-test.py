import json
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.http_operator import SimpleHttpOperator
from datetime import datetime, timedelta
import os

os.environ['AIRFLOW_CONN_LIVY'] = 'http://spark-test-livy.spark-test'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 27),
    'email': ['arunm@cloudera.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'spark-test',
    default_args=default_args,
    description='Spark test DAG',
    schedule_interval='*/1 * * * *',
    catchup=False
)

t1 = BashOperator(
    task_id='date',
    bash_command='date',
    dag=dag,
)

t2 = SimpleHttpOperator(
    task_id='spark-pi',
    http_conn_id='LIVY',
    endpoint='batches',
    data=json.dumps({"file": "local:///opt/spark/examples/jars/spark-examples_2.11-2.4.2.jar", "className": "org.apache.spark.examples.SparkPi", "args": ["5000"]}),
    headers={"Content-Type": "application/json"},
    dag=dag,
)

t2.set_upstream(t1)
