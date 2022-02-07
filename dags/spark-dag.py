from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

#Base parameters of spark
spark_master = "spark://spark:7077"
spark_app_name = "Spark book count"
file_path = "/usr/local/spark/data/book.txt"

#create a datetime variable
now = datetime.now()

#Default arguments
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

#Define the DAG
dag = DAG(
        "spark-book-count", 
        default_args=default_args, 
        schedule_interval=timedelta(1)
    )

#First task
start = DummyOperator(task_id="start", dag=dag)

#Second task
spark_job = SparkSubmitOperator(
    task_id="spark_job",
    application= "/usr/local/spark/app/spark-book-count.py", # Spark application path created in airflow
    name=spark_app_name,
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    application_args=[file_path],
    dag=dag)

#Third task
end = DummyOperator(task_id="end", dag=dag)

#Create the dependencies 
start >> spark_job >> end