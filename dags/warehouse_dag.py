from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG for daily job
with DAG('user_etl_daily', default_args=default_args, schedule_interval='@daily') as dag_daily:
    user_etl = SparkSubmitOperator(
        task_id='user_etl',
        application='/scripts/user_etl.py',
        conn_id='spark_default',
        dag=dag_daily
    )

# DAG for hourly job
with DAG('product_etl_hourly', default_args=default_args, schedule_interval='@hourly') as dag_hourly:
    product_etl = SparkSubmitOperator(
        task_id='product_etl',
        application='/scripts/product_etl.py',
        conn_id='spark_default',
        dag=dag_hourly
    )

# DAG for 5-minute interval job
with DAG('order_etl_5min', default_args=default_args, schedule_interval='*/5 * * * *') as dag_5min:
    order_etl = SparkSubmitOperator(
        task_id='order_etl',
        application='/scripts/order_etl.py',
        conn_id='spark_default',
        dag=dag_5min
    )