from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from spark_streaming import data_streaming
from creep import collect_data
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(weeks=1)
}

with DAG(
    dag_id = 'streaming_dag',
    default_args = default_args,
    schedule = '@daily',
    catchup = False
):

    get_data_from_api_to_kafka = PythonOperator(
        task_id = 'get_data_from_api_to_kafka',
        python_callable = collect_data()
    )

    spark_streaming = PythonOperator(
        task_id = 'spark_streaming',
        python_callable = data_streaming()
    )

    get_data_from_api_to_kafka >> spark_streaming
