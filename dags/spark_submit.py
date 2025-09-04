from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import datetime

with DAG(
    dag_id="spark_submit_via_bash",
    start_date=datetime(2025, 6, 1),
    schedule=None,        # Manual trigger only
    catchup=False,
    tags=["spark", "bash"],
) as dag:

    spark_submit_task = BashOperator(
        task_id="spark_submit_job",
        bash_command="""
#!/bin/bash
spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.eventLog.enabled=false \
  /opt/airflow/dags/hello_spark.py
"""
    )

    spark_submit_task

