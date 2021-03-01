#!/usr/bin/python3
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# let's setup arguments for our dag


# dag declaration

dag = DAG(dag_id='platform.airflow-test',
          description='',
          schedule_interval="0 0 * * *",
          start_date=datetime(2020, 7, 1),
          max_active_runs=1,
          catchup=True,
          dagrun_timeout=timedelta(minutes=2))

run_this = BashOperator(
    task_id='run_after_loop',
    bash_command=' for((i=1;i<=600;i+=1)); do echo "Welcome $i times"; sleep 1;  done',
    dag=dag,
)