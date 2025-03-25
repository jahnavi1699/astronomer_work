from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Define your Python function that will be called by the PythonOperator
def print_hello():
    print("hello from Airflow v2!")


# Define the DAG
dag = DAG(
    'python_operator_example',  # DAG name
    description='A simple PythonOperator example',
    schedule_interval=None,  # None means it will not run on its own schedule
    start_date=datetime(2024, 3, 24),  # Start date for the DAG
    catchup=False,  # If set to False, Airflow will not try to "catch up" for missed schedules
)

# Define the PythonOperator task
task = PythonOperator(
    task_id='print_hello_task',  # Unique task ID
    python_callable=print_hello,  # The function to execute
    dag=dag,  # The DAG the task belongs to
)

# Set the task in the DAG
task