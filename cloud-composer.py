import os
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Python logic to derive yesterdays date
YESTERDAY = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())


#Default args
default_args = {

   'start_date': YESTERDAY,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)

}

# Python custom logic/function

def print_hello():
    print('Hey i am python operator')


#DAG Defenations

with DAG(
   dag_id = 'basic python operator demo',
   catchup  = False,
   schedule_interval = timedelta(days=1),
   default_args = default_args
   ) as dag:

 # Dummy Start task
 start = DummyOperator(
     task_id ='start',
     dag=dag,
 )

#bash operator task
 bash_task = bashOperator(
          task_id = 'bash_task',
          bash_command = "date:echo 'hey i am bash operator'"
         )

 # Python operator task
 python_task = PythonOperator(
     task_id='Python_task',
     python_callable=print_hello,
     dag=dag
 )

 # dummy end task
 end = DummyOperator(
     task_id='end',
     dag=dag
 )

 start >>  bash_task >> python_task >> end
