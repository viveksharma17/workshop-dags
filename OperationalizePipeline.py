import airflow
from airflow import DAG
from datetime import datetime, timedelta

# Importing Qubole Operator in DAG
from airflow.contrib.operators.qubole_operator import QuboleOperator
from airflow.contrib.hooks.qubole_hook import COMMAND_ARGS
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


import json

# Following are defaults which can be overridden later on
default_args = {
   'owner': 'REPLACE',
   'depends_on_past': False,
   'start_date': airflow.utils.dates.days_ago(0),
   'email': ['REPLACE'],
   'email_on_failure': True,
   'email_on_retry': True,
   'retries': 1,
   'retry_delay': timedelta(minutes=1),
}

dag = DAG('Workshop-ML-Model-REPLACE', default_args=default_args)
start = DummyOperator(
    task_id='start',
    dag=dag
)



ingestData = QuboleOperator(
     task_id='ingestData',
     command_type="sparkcmd",
     note_id="1271",
     qubole_conn_id='qubole_default',

     dag=dag)

analyze_data = QuboleOperator(
     task_id='analyze_data',
     command_type="sparkcmd",
     note_id="1274",
     qubole_conn_id='qubole_default',

     dag=dag)


# Spark Command - Run a Notebook
build_dashboards = QuboleOperator(
     task_id='build_dashboards',
     command_type="sparkcmd",
     note_id="1273",
     qubole_conn_id='qubole_default',

     dag=dag)

# Spark Command - Run a Notebook
ML_Models = QuboleOperator(
     task_id='ML_Models',
     command_type="sparkcmd",
     note_id="1272",
     qubole_conn_id='qubole_default',

     dag=dag)
start >> ingestData >> analyze_data >> build_dashboards >> ML_Models
