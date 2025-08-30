import logging
import pandas as pd

from datetime import datetime, timedelta
from typing import Dict, List
from tempfile import NamedTemporaryFile

from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

# The BranchPythonOperator - is like a conditional oprator. Trigger task_id based on the return data.
def check_table_exists():
      try:
            hook = PostgresHook(postgres_conn_id="local_postgres_conn_id")
            connection = hook.get_conn()
            cursor = connection.cursor()

            # Check if table exists and has data
            cursor.execute("""        SELECT order_id FROM orders LIMIT 1;          """)

            table_has_data = cursor.fetchone()
            logging.info(f"TABLE EXISTS RESULT ==> {table_has_data}")
            return "skip_insert" if table_has_data else "write_data_into_orders_table"
      except Exception as e:
            logging.error(e)
      
      finally:
            cursor.close()
            connection.close()

def write_data_into_table():
      hook = PostgresHook(postgres_conn_id = "local_postgres_conn_id")
      connection = hook.get_conn()
      cursor = connection.cursor()

      orders_data = pd.read_csv("/opt/airflow/data/Orders.csv")
      print("TOTAL_NUMBER OF ROWS ==> ", len(orders_data))
      for _, current_row in orders_data.iterrows():
            cursor.execute("INSERT INTO orders VALUES (%s, %s, %s, %s)", 
                           (
                              current_row.order_id, current_row.date,
                              current_row.product_name, current_row.quantity
                        ))
      connection.commit()     # Without commit the transaction is not loaded into the table
      cursor.close()
      connection.close()

def read_db_write_s3(ds_nodash, next_ds_nodash):
      # ds_nodash       - Execution Date without Dash ==> YYMMDD
      # next_ds_nodash  - Next Execution Date Without Dash if exisits else None
      try:
            hook = PostgresHook(postgres_conn_id = "local_postgres_conn_id")
            connection = hook.get_conn()
            cursor = connection.cursor()

            columns_to_fetch: List = ["order_id", "date", "product_name", "quantity"]
            
            cursor.execute(
                              f" SELECT {', '.join(columns_to_fetch)} \
                              FROM orders \
                              WHERE date >= '2025-08-30' AND date <= '2025-10-31'"
                        )
            result = cursor.fetchall()
            
            result_df = pd.DataFrame(result, columns=columns_to_fetch)
            print(result_df.head(2))

            with NamedTemporaryFile(suffix=".csv", mode="w") as temp_file:
                  result_df.to_csv(temp_file.name, index=False)

                  hook = S3Hook(aws_conn_id = "minio_connection_id",)
                  hook.load_file(
                        filename = temp_file.name,
                        bucket_name = "airflowbucket",
                        key = f"orders/{ds_nodash}.txt",
                        replace =True
                  )
                  logging.info("DATA PUSHED TO BUCKET")
            
      except Exception as e:
            print(e)

      finally:
            cursor.close()
            connection.close()
            

default_args: Dict = {
      "owner" : "vishnnu",
      "retries" : 5,
      "retry_delay" : timedelta(minutes=5)
}

with DAG (
      dag_id = "psql_s3_hooks_dag",
      default_args = default_args,
      start_date = datetime(2025, 8, 29),
      schedule = "0 0 * * *",
      catchup = False
) as dag: 
      
      create_orders_table_task = PostgresOperator(
            task_id = "create_orders_table_task",
            postgres_conn_id = "local_postgres_conn_id",
            sql = """
                        CREATE TABLE IF NOT EXISTS orders(
                              order_id varchar(20),
                              date date,
                              product_name character varying,
                              quantity integer,
                              PRIMARY KEY (order_id)
                        )
                  """
      )

      # The BranchPythonOperator - is like a conditional oprator. Trigger task_id based on the return data.
      check_table_task = BranchPythonOperator(
            task_id = "check_table_task",
            python_callable = check_table_exists
      )

      insert_into_table_task = PythonOperator(
            task_id = "write_data_into_orders_table",
            python_callable = write_data_into_table
      )

      skip_insert_task = PythonOperator(
            task_id = "skip_insert",
            python_callable = lambda: print("Table already has data - skipping insert")
      )

      read_db_write_s3_task = PythonOperator(
            task_id = "read_db_write_s3",
            python_callable = read_db_write_s3
      )

      create_orders_table_task >> check_table_task
      check_table_task >> [insert_into_table_task, skip_insert_task] >> read_db_write_s3_task