import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

import sys
sys.path.append('/usr/local/airflow/scripts')
sys.path.append('/usr/local/airflow/plugins/operators')
from data_quality import PostgresDataQualityOperator
import sql_queries

default_args = {
    'owner' : 'pathairs',
    'retries'  : 1,
    'retry_delay' : datetime.timedelta(minutes=3),
}

dag = DAG(
    'hands_on_test',
    default_args = default_args,
    description = 'lineman wongnai data engineer test',
    start_date = days_ago(1),
    schedule_interval = '@daily'
)

start = DummyOperator(
    task_id = 'start',
    dag = dag
)

drop_order_detail = PostgresOperator(
    task_id = 'drop_order_detail_table',
    dag = dag,
    postgres_conn_id = 'postgres_db',
    sql = sql_queries.drop_order_detail_table,
    autocommit = True
)

create_order_detail = PostgresOperator(
    task_id = 'create_order_detail',
    dag = dag,
    postgres_conn_id = 'postgres_db',
    sql = sql_queries.create_order_detail_table,
    autocommit = True
)

copy_order_detail_data = PostgresOperator(
    task_id = 'copy_order_detail_data',
    dag = dag,
    postgres_conn_id = 'postgres_db',
    sql = sql_queries.copy_order_detail_data,
    autocommit = True
)

drop_restaurant_detail = PostgresOperator(
    task_id = 'drop_restaurant_detail_table',
    dag = dag,
    postgres_conn_id = 'postgres_db',
    sql = sql_queries.drop_restaurant_detail_table,
    autocommit = True
)

create_restaurant_detail = PostgresOperator(
    task_id = 'create_restaurant_detail',
    dag = dag,
    postgres_conn_id = 'postgres_db',
    sql = sql_queries.create_restaurant_detail_table,
    autocommit = True
)

copy_restaurant_detail_data = PostgresOperator(
    task_id = 'copy_restaurant_detail_data',
    dag = dag,
    postgres_conn_id = 'postgres_db',
    sql = sql_queries.copy_restaurant_detail_data,
    autocommit = True
)

postgres_data_quality_check = PostgresDataQualityOperator(
    task_id = 'postgreq_data_quality_check',
    dag = dag,
    postgres_conn_id = 'postgres_db',
    data_quality_checks = sql_queries.postgres_data_quality_check
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

spark_transform_order_table = BashOperator(
    task_id = 'spark_transform_order_table',
    dag = dag,
    bash_command = 'docker exec spark-master /spark/bin/spark-submit --master local[*] --name spark_transform_order_table /home/script/transform_order_table.py '
)

spark_transform_restaurant_table = BashOperator(
    task_id = 'spark_transform_restaurant_table',
    dag = dag,
    bash_command = 'docker exec spark-master /spark/bin/spark-submit --master local[*] --name spark_transform_restaurant_table /home/script/transform_restaurant_table.py '
)

create_hive_order_detail = BashOperator(
    task_id = 'create_hive_order_detail',
    dag = dag,
    bash_command = 'docker exec hive-server hive -f /opt/hql/order_detail.hql '
)

create_hive_restaurant_detail = BashOperator(
    task_id = 'create_hive_restaurant_detail',
    dag = dag,
    bash_command = 'docker exec hive-server hive -f /opt/hql/restaurant_detail.hql '
)

spark_create_order_detail_new = BashOperator(
    task_id = 'spark_create_order_detail_new',
    dag = dag,
    bash_command = 'docker exec spark-master /spark/bin/spark-submit --master local[*] --name spark_create_order_detail_new /home/script/create_order_detail_new.py '
)

spark_create_restaurant_detail_new = BashOperator(
    task_id = 'spark_create_restaurant_detail_new',
    dag = dag,
    bash_command = 'docker exec spark-master /spark/bin/spark-submit --master local[*] --name spark_create_restaurant_detail_new /home/script/create_restaurant_detail_new.py '
)

create_hive_order_detail_new = BashOperator(
    task_id = 'create_hive_order_detail_new',
    dag = dag,
    bash_command = 'docker exec hive-server hive -f /opt/hql/order_detail_new.hql '
)

create_hive_restaurant_detail_new = BashOperator(
    task_id = 'create_hive_restaurant_detail_new',
    dag = dag,
    bash_command = 'docker exec hive-server hive -f /opt/hql/restaurant_detail_new.hql '
)

generate_sql_requirement = BashOperator(
    task_id = 'generate_sql_requirement',
    dag = dag,
    bash_command = 'docker exec spark-master /spark/bin/spark-submit --master local[*] --name spark_create_restaurant_detail_new /home/script/sql_requirement.py '
)

spark_data_quality_check = BashOperator(
    task_id = 'spark_data_quality_check',
    dag = dag,
    bash_command = 'docker exec spark-master /spark/bin/spark-submit --master local[*] --name spark_data_quality_check /home/script/dq_check.py '
)

end = DummyOperator(
    task_id = 'end',
    dag = dag
)
start >> drop_order_detail >> create_order_detail >> copy_order_detail_data >> postgres_data_quality_check
start >> drop_restaurant_detail >> create_restaurant_detail >> copy_restaurant_detail_data >> postgres_data_quality_check
postgres_data_quality_check >> install_sqoop >> import_sqoop >> [ spark_transform_order_table, spark_transform_restaurant_table]

spark_transform_order_table >> spark_create_order_detail_new >> spark_data_quality_check >> [create_hive_order_detail , create_hive_order_detail_new]
spark_transform_restaurant_table >> spark_create_restaurant_detail_new >> spark_data_quality_check >> [create_hive_restaurant_detail , create_hive_restaurant_detail_new]
[create_hive_order_detail, create_hive_restaurant_detail, create_hive_order_detail_new, create_hive_restaurant_detail_new] >> generate_sql_requirement >> end