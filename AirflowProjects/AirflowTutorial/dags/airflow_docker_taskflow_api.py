from datetime import datetime, timedelta
from typing import Dict

from airflow.decorators import dag, task

default_args: Dict = {
                        "owner" : "vishnnu",
                        "retries" : 5,
                        "retry_delay" : timedelta(minutes=2)
                  }

@dag(dag_id = "task_flow_api_dag_1", description = "First task flow API dag",
     default_args = default_args,  schedule = "@daily",
     start_date = datetime(2025, 8, 10), catchup = False)
def task_flow_api_dag_1():
      
      # The multiple_outputs parameter passed -> Also stores the data in x_comm. Compound DataType - Set True.
      @task(multiple_outputs = True)
      def get_name():
            return {"first_name" : "Rajalakshmi", "last_name" : "Vishnnu"}

      @task()
      def get_age():
            return 25

      @task()
      def log_name_and_age(age: int, first_name: str, last_name: str) -> str:
            return f"I am is {first_name} {last_name} and i'm {age} years old."

      # Task Dependencies and flow:
      first_and_last_name: Dict = get_name()
      age: int = get_age()
      output = log_name_and_age(age, first_and_last_name["first_name"], first_and_last_name["last_name"])
      
dag_1 = task_flow_api_dag_1()