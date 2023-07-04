import pendulum
from random import randint
from airflow import DAG
from airflow.decorators import dag,task

def mathOperation():
    number=randint(1,10)*randint(1,10)
    return number

def higherNumber(x: int,y:int):
    return max(x,y)

@dag(
    dag_id="SimpleMathOperationDataFlowAPI",
    description="This is a simple math operation to compare 2 random numbers using the dataflow API",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2023,7,4)
)
def taskflow_dag():
    @task
    def MathOperation1():
        return mathOperation
        
    @task
    def MathOperation2():
        return mathOperation

    @task
    def selectHigherNumberOperation(num: int,num1:int):
        return (higherNumber(num,num1))

    selectHigherNumberOperation(MathOperation1,MathOperation2)

taskflow_dag()