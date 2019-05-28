from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 20),
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

t2 = BashOperator(
    task_id='spark-pi',
    bash_command='curl -H "Content-Type: application/json" -X POST -d "{\"file\": \"local:///opt/spark/examples/jars/spark-examples_2.11-2.4.2.jar\", \"className\": \"org.apache.spark.examples.SparkPi\", \"args\": [\"5000\"]}" "http://spark-test-livy.spark-test/batches"',
    dag=dag,
)

t2.set_upstream(t1)
