#!/usr/bin/python3
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# let's setup arguments for our dag

default_dag_id = "dag_02"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 10,
    'concurrency': 1,
    'email': ['f89cb3db.mti365.onmicrosoft.com@apac.teams.ms']
}

# dag declaration

dag = DAG(
    dag_id=default_dag_id,
    default_args=default_args,
    start_date=datetime(2019, 6, 17),
    email: [''],
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
