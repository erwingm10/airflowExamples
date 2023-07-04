import pendulum
from random import randint
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

def mathOperation(ti):
    number=randint(1,10)*randint(1,10)
    ti.xcom_push(key="result",value=number)
    return number

def higherNumber(ti):
    return max(
        ti.xcom_pull(key="result",task_ids="MathOperation1"),
        ti.xcom_pull(key="result",task_ids="MathOperation2")
        )

with DAG(
    dag_id="SimpleMathOperation",
    description="This is a simple math operation to compare 2 random numbers",
    start_date=pendulum.datetime(2023,7,4),
    schedule="0 0 * * *",
    catchup=False
) as dag:
    task1=PythonOperator(task_id="MathOperation1",python_callable=mathOperation)
    
    task2=PythonOperator(task_id="MathOperation2",python_callable=mathOperation)

    task3=PythonOperator(
        task_id="higherNumberCompare",
        python_callable=higherNumber
    )


    [task1,task2] >> task3