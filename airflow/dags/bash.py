import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator

import sys
sys.path.append('/usr/local/airflow/scripts')
sys.path.append('/usr/local/airflow/plugins/operators')
from data_quality import PostgresDataQualityOperator
import sql_queries

default_args = {
    'owner' : 'pathairs'
}

dag = DAG(
    'ssh_pipeline',
    default_args = default_args,
    description = 'lineman wongnai data engineer test',
    start_date = datetime.datetime(2021, 4, 21, 0, 0, 0)
)

install_sqoop = BashOperator(
    task_id = 'install_sqoop',
    bash_command = 'docker exec hive-server bash /opt/sqoop/install_sqoop.sh ',
    dag = dag
)

import_sqoop = BashOperator(
    task_id = 'import_sqoop',
    bash_command = 'docker exec hive-server bash /opt/sqoop/import_sqoop.sh ',
    dag = dag
)

list_sqoop_data = BashOperator(
    task_id = 'list_sqoop_data',
    bash_command = 'docker exec hive-server hdfs dfs -ls /user/sqoop ',
    dag = dag
)

install_sqoop >> import_sqoop >> list_sqoop_data