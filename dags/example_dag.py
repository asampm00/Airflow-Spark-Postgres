"""
Author: kamrul Hasan
Date: 18.04.2022
Email: hasan.alive@gmail.com
"""


"""
This pipeline will transform the data little bit using some aggregation by pulling the data from postgres and then generate a static html report. 
"""

import json
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
import urllib3
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

now = datetime.now()
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

#define the dag
dag = DAG(
    dag_id="example-task",
    description="This dag does very simple etl with the help of docker, pyspark and postgres.",
    default_args=default_args,
    schedule_interval=timedelta(1)
)

start = DummyOperator(task_id="start", dag=dag)

example_spark_job = SparkSubmitOperator(
    task_id="example_spark_job",
    application="/usr/local/spark/src/app/example.py",
    name="example_spark_job",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master": "local[*]"},
    py_files='/usr/local/spark/src/app/example.py',
    dag=dag)


end = DummyOperator(task_id="end", dag=dag)

# create the dependency chain
start >> example_spark_job >> end
