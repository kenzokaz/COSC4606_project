from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}


with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["etl", "spark"],
) as dag:

    spark_transform = BashOperator(
        task_id="spark_transform",
        bash_command=(
            "docker exec spark-master "
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "/data/services/transformer/job.py"
        ),
    )

    spark_transform
