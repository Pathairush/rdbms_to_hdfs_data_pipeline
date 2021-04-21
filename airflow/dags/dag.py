from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
import datetime

default_args = {
    'owner' : 'pathairs'
}

dag = DAG(
    'example_dag',
    default_args = default_args,
    description = 'lineman wongnai data engineer test',
    start_date = datetime.datetime(2021, 4, 21, 0, 0, 0)
)

task = DummyOperator(
    task_id = 'test_airflow_configuration',
    dag = dag
)