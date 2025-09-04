from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from pendulum import datetime

def hello_python():
    print(" Hello from Python task")

with DAG(
    dag_id="simple_airflow3_dag",
    start_date=datetime(2025, 6, 1),
    schedule="@daily",
    catchup=False,
    tags=["example"],
) as dag:

    # Task 1: Print from Python
    t1 = PythonOperator(
        task_id="hello_python",
        python_callable=hello_python,
    )

    # Task 2: Print from Bash
    t2 = BashOperator(
        task_id="hello_bash",
        bash_command='echo "Hello from Bash task"',
    )

    # Set execution order
    t1 >> t2

