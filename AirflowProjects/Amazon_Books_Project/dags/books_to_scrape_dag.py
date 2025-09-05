from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from get_books_data import *

default_args: Dict = {
      "owner" : "vishnnu",
      "retries" : 2,
      "retry_delay" : timedelta(minutes=3)
}

dag: object = DAG(
                              dag_id = "books_to_scrape_dag",
                              default_args = default_args,
                              start_date = datetime(2025, 9, 1),
                              schedule = "0 0 * * *",
                              catchup = False
                        )

create_postgres_books_table_task = PostgresOperator(
      task_id = "create_postgres_books_table",
      postgres_conn_id = "postgres_connection_key",
      sql = """
                  CREATE TABLE IF NOT EXISTS books_to_scrape (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        title character varying,
                        url character varying,
                        rating varchar(10),
                        availability varchar(15),
                        currency varchar(5),
                        price integer,
                        description character varying,
                        count integer
                  )
            """,
      dag = dag
)

fetch_all_books_task = PythonOperator(
      task_id = "fetch_all_books_from_site",
      python_callable = get_all_books_and_write_to_s3,
      dag = dag,
      # op_kwargs = {},
)

insert_into_postgres_db_task = PythonOperator(
      task_id = "insert_into_postgres_database",
      python_callable = insert_into_postgres_database,
      dag = dag
)

fetch_all_books_task >> create_postgres_books_table_task >> insert_into_postgres_db_task