"""
      Airflow is an open source tool to create, schedule and monitor various workflows.
      =================================================================================
	* Based out of python. (Written in Python)
	* 2014 - AirBnB to manage complex workflows.
	* One of the workflow managment platforms
	
	
	Workflow -> Task -> Operator:
	-----------------------------
		* Workflow -> collection of Tasks (Also called as DAG in Airflow).
		* Task     -> Is an unit of work in the DAG. Represents a Node in the DAG graph.
						A task can be a dependency of multiple task in the DAG representation.
						++ Implementation of an operator.
		* Operator -> Is the actual work that is being done inside the task.
							Python Operator
							Bash Operator
							Customised Operator

      Task Workflow (Happy Workflow):
      ------------------------------- 
            NO_STATUS ->> (SCHEDULER) -> SCHEDULED ->> (EXECUTOR) -> QUEUED ->> (WORKER) -> RUNNING -> SUCCEEDED.
            
      Task Dependency Method Creation:
      ================================
            * task_1.set_downstream([task_2, task_3])
            * task_3.set_downstream([task_4])
                        (or)
            * task_1 >> [task_2, task_3]
            * task_3 >> task_4
            
            * [task_4, task_5] >> task 6 // This is also possible.

      
      * Version 2.0+ airflow backfill -s <start_date> -e <end_date> <dag_id>

      There are five inputs given to have a cron expression:
      ------------------------------------------------------
	   0		 0 			*			  *			    *			=> @daily		 
	Minutes	Hour 	       Day(Month)		       Month		 Day(Week)
"""


from datetime import datetime, timedelta
from typing import Dict

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args: Dict = {
                        "owner" : "Vishnnu",
                        "retries" : 3,
                        "retry_delay" : timedelta(minutes=2)
                  }

def get_input_params(name: str, age: int) -> None:
      print(f"Hello, my name is {name} and i'm {age} years old")

# The X_COM (CROSS-COMMUNICATION) is used to share values across tasks. Note: Can store values only upto 48KB.
# ti => TaskInstance object, which provides access to the task's runtime context, including XComs.

def push_fullname_into_xcom(ti):
      ti.xcom_push(key = "first_name", value = "Raaaaji")
      ti.xcom_push(key = "last_name",  value = "Vishnnu")

def push_age_into_xcom(ti):
      ti.xcom_push(key="age", value=25)

def pull_details_from_xcom(ti, married_since = '15-07-2025'):
      first_name: str = ti.xcom_pull(task_ids = "push_fullname_into_xcom", key = "first_name")
      last_name: str = ti.xcom_pull(task_ids = "push_fullname_into_xcom", key = "last_name")

      age: str = ti.xcom_pull(task_ids = "push_age_into_xcom", key = "age")
      print(f"Hello!! My name is {first_name + last_name} and i'm {age} years old. I got married on {married_since}")
      return first_name, last_name, age, married_since


with DAG(
      dag_id = "first_dag_created",
      description = "Vishnnus first dag created using Airflow",
      start_date = datetime(2025, 8, 24, 10),
      schedule = "@daily",                      # schedule_interval = "@daily" -> Depricated from (2.2+)
      catchup = False,        # To avoid backfilling
      tags = ["Vishnnu_First_DAG", "BASH_OPERATOR"],
      default_args = default_args
) as first_dag_created:

      task_1 = BashOperator(
            task_id = "First_Bash_Task",
            bash_command = "echo Bash Operator First Task"
      )

      task_2 = BashOperator(
            task_id = "Second_Bash_Task",
            bash_command = "echo Bash Operator Second Task"
      )

      task_3 = BashOperator(
            task_id = "Third_Bash_Task",
            bash_command = "echo Bash Operator Third Task"
      )
      
      task_4 = BashOperator(
            task_id = "Fourth_Bash_Task",
            bash_command = "echo Bash Operator Fourth Task"
      )

      python_task_1 = PythonOperator(
            task_id = "get_input_params",
            python_callable = get_input_params,
            op_kwargs = {"name" : "Vishnnu", "age" : 26}
      )

      python_task_2 = PythonOperator(
            task_id = "push_fullname_into_xcom",
            python_callable = push_fullname_into_xcom
      )

      python_task_3 = PythonOperator(
            task_id = "push_age_into_xcom",
            python_callable = push_age_into_xcom
      )

      python_task_4 = PythonOperator(
            task_id = "pull_details_from_xcom",
            python_callable = pull_details_from_xcom,
            op_kwargs = {"married_since" : "16-07-2025"}
      )

      # Task Dependecy Method 1:
      task_1.set_downstream([task_2, task_3])
      task_3.set_downstream([task_4])

      task_2.set_downstream([python_task_1])

      python_task_1 >> [python_task_2, python_task_3]
      [python_task_2, python_task_3] >> python_task_4

      # Task Dependecy Method 2:
      # task_1 >> [task_2, task_3]