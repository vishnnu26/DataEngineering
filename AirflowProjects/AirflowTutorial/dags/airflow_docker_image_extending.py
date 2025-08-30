from datetime import datetime, timedelta
from typing import Dict

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator

default_args: Dict = {
                        "owner" : "vishnnu",
                        "retries" : 3,
                        "retry_delta" : timedelta(minutes=1)
                  }

# This is after importing the requirements.txt file in the Dockerfile.
@dag(dag_id = "extending_airflow_dag",
     default_args = default_args,
     start_date = datetime(2025, 8, 25),
     schedule = "0 0 * * *",
     catchup = False
)
def extending_airflow_dag():
      @task()
      def run_scikit_learn_package():
            import sklearn
            print("scikit-learn package is successfully imported", sklearn.__version__)

      @task()
      def run_pandas_and_matplotlib_package():
            import matplotlib
            import pandas
            print("matplotlib package is successfully imported", matplotlib.__version__)
            print("pandas package is successfully imported", pandas.__version__)
      
      run_scikit_learn_package()
      run_pandas_and_matplotlib_package()

extending_airflow_dag = extending_airflow_dag()