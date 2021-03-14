#!/usr/bin/python3
import os
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

# let's setup arguments for our dag

default_dag_id = "dag_02"


global_default_args = {
    'owner': 'etl',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': [ str(os.environ.get('ETL_SENDGRID_MAIL_TO')) ]
}

default_args = {
  **global_default_args
}

# dag declaration

dag = DAG(
    dag_id=default_dag_id,
    default_args=default_args,
    start_date=datetime(2019, 6, 17),
    catchup=False
)


# Here's a task based on Bash Operator!

def print_context(ds, **kwargs):
    """Print the Airflow context and ds variable from the context."""
    pprint(kwargs)
    print(ds)
    raise Exception("Testing Exception!")
    return 'Whatever you return gets printed in the logs'


run_this = PythonOperator(
    task_id='print_the_context',
    python_callable=print_context,
    dag=dag,
)
