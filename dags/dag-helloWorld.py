import pendulum
from random import randint

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="helloWorldSimple",
    description="This is a simple hello world dag",
    schedule="0 0 * * * ",
    catchup=False,
    start_date=pendulum.datetime(2023,7,4,tz="UTC")
) as dag:
    task1=EmptyOperator(task_id="start")

    task2=BashOperator(task_id="helloWorldPrint",bash_command="echo Hello World")

    task1 >> task2