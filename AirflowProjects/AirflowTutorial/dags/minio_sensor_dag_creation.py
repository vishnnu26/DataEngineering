from datetime import datetime, timedelta
from typing import Dict

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

default_args: Dict = {
                        "owner" : "vishnnu",
                        "retires" : 5,
                        "retry_delay" : timedelta(minutes=3)
                  }

with DAG(
      dag_id = "minio_sensor_dag_creation",
      default_args = default_args,
      start_date = datetime(2025, 8, 29),
      schedule = "0 0 * * *",
      catchup = False
) as dag:
      # This is to Check if a S3 object is present in the prefix.
      minio_poke_task = S3KeySensor(
            task_id = "minio_sensor_poke_check",
            bucket_name = "airflowbucket",
            bucket_key = "data.csv",
            mode = "poke",
            poke_interval = 10,
            timeout = 50,
            aws_conn_id = "minio_connection_id",
            verify=False
      )

      minio_poke_task