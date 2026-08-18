from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3

def upload_sample():
    s3 = boto3.client('s3')
    s3.upload_file('/path/to/sample_posts.csv', 'reddit-raw-data', 'sample_posts.csv')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG('reddit_kafka_sentiment_dag',
         default_args=default_args,
         schedule_interval="@daily",
         catchup=False) as dag:

    upload_to_s3 = PythonOperator(
        task_id='upload_sample',
        python_callable=upload_sample
    )

    run_kafka = BashOperator(
        task_id='run_kafka_producer',
        bash_command='python3 /path/to/kafka/kafka_producer.py'
    )

    run_spark = BashOperator(
        task_id='run_spark_streaming',
        bash_command='spark-submit /path/to/spark_jobs/spark_streaming_job.py'
    )

    upload_to_s3 >> run_kafka >> run_spark
