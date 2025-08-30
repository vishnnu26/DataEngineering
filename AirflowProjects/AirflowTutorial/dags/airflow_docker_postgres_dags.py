from datetime import datetime, timedelta
from typing import Dict

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args: Dict = {
                        "owner" : "vishnnu",
                        "retries" : 3,
                        "retry_delta" : timedelta(minutes=1)
                  }

with DAG(
      dag_id = "postgres_connection_dag",
      default_args = default_args,
      start_date = datetime(2025, 8, 25),
      schedule = "0 0 * * *",
      catchup = False
) as dag:
      
      create_table_task = PostgresOperator(
            task_id = "create_table_dags_runs",
            postgres_conn_id = "local_postgres_conn_id",
            sql = """
                        CREATE TABLE IF NOT EXISTS dags_runs (
                                                      dt date,
                                                      dag_id character varying,
                                                      PRIMARY KEY (dt, dag_id)
                                                )
                  """
      )

      insert_data_into_table_task = PostgresOperator(
            task_id = "insert_data_into_dags_run_table",
            postgres_conn_id = "local_postgres_conn_id",
            sql = """
                        INSERT INTO dags_runs VALUES ('{{ ds }}', '{{ dag.dag_id }}')
                  """
      )

      # AIRFLOW MACROS PAGE -> {ds => current execution date} || {dag.dag_id}
      delete_data_from_table_task = PostgresOperator(
            task_id = "delete_from_dags_run_table",
            postgres_conn_id = "local_postgres_conn_id",
            sql = """
                        DELETE 
                        FROM dags_runs
                        WHERE dt = '{{ ds }}' AND dag_id = '{{ dag.dag_id }}'
                  """
      )

      # Task Dependencies and flow:
      create_table_task >> delete_data_from_table_task >> insert_data_into_table_task