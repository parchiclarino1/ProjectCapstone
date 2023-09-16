from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Define default arguments for the DAG
default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Create a DAG instance
dag = DAG(
    'sample_dummy_dag',
    default_args=default_args,
    schedule_interval="0 * * * *",  # Set the schedule interval (None for manual execution)
    catchup=False,  # If set to False, only run the most recent task instances
)

# Define tasks in the DAG
start_task = DummyOperator(task_id='start_task', dag=dag)

# Define a Python function to be executed by the PythonOperator
def print_hello_world():
    print("Hello, world!")

hello_world_task = PythonOperator(
    task_id='hello_world_task',
    python_callable=print_hello_world,
    dag=dag,
)

end_task = DummyOperator(task_id='end_task', dag=dag)

# Define the task dependencies
start_task >> hello_world_task >> end_task
