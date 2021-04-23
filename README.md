# Lineman Wongnai Data Engineer Test

Hi Lineman Wongnai team :wave: , Thanks for giving me the opportunity to take a test for data engineer position. Here is the summary of this project.

# Required

1. Docker desktop
2. Docker compose

# How to run this project
I provide you a docker-compose file so that you can run the whole application with the following command.

```
docker-compose up -d
```

Then you can access the Airflow UI webserver through port 8080
- http://localhost:8080

Please feel free to turn the dag button on for the `hands_on_test`.
I set the start date of the dag to be 23 April 2021. It set to run on a daily basis.

Assume that the pipeline is run completely. You can test the result on the following architectures like this.

## PostgresSQL

```
# show table in database
docker exec postgres-db psql -U postgres -d lineman_wongnai -c \\dt
```
```
# describe table
docker exec postgres-db psql -U postgres -d lineman_wongnai -c "
SELECT 
   table_name, 
   column_name, 
   data_type 
FROM 
   information_schema.columns
WHERE 
   table_name = '<<TARGET TABLE>>';
"
# change <<TARGET TABLE>> to your table name e.g., 'order_detail', 'restaurant_detail'
```
```
# sample data
docker exec postgres-db psql -U postgres -d lineman_wongnai -c "SELECT * FROM <<TARGET TABLE>> LIMIT 5;"
"

# change <<TARGET TABLE>> to your table name e.g., 'order_detail', 'restaurant_detail'
```

## HIVE

```
# show tables
docker exec hive-server beeline -u jdbc:hive2://localhost:10000/default -e "SHOW TABLES;"
```

```
# describe table
docker exec hive-server beeline -u jdbc:hive2://localhost:10000/default -e "SHOW CREATE TABLE <<TARGET TABLE>>;"

# change <<TARGET TABLE>> to your table name e.g., 'order_detail', 'restaurant_detail'
```

```
# sample data
docker exec hive-server beeline -u jdbc:hive2://localhost:10000/default -e "SELECT * FROM <<TARGET TABLE>> LIMIT 5;"

# change <<TARGET TABLE>> to your table name e.g., 'order_detail', 'restaurant_detail'
```

```
# check partitioned parquet
docker exec hive-server hdfs dfs -ls /user/spark/transformed_order_detail
docker exec hive-server hdfs dfs -ls /user/spark/transformed_restaurant_detail

# check the source of external in /airflow/scripts/hql script.
```

For SQL requirement files, the CSV files will be placed in the `./sql_result` when the dag is completed.

After you finish the test, you can close the whole application by
```
docker-compose down -v
```



#  Business requirement
1. Create two tables in postgres database with the above given column types.
    - [X] order_detail table using `order_detail.csv`
    - [X] restaurant_detail table using `restaurant_detail.csv`
    - Check `COPY` command in `airflow/dags/script/sql_queries.py`

2. Once we have these two tables in postgres DB, ETL the same tables to Hive with the same names and corresponding Hive data type using the below guidelines
    - [X] Both the tables should be `external table`
    - [X] Both the tables should have `parquet file format`
    - [X] restaurant_detail table should be partitioned by a column name `dt` (type string) with a static value `latest`
    - [X] order_detail table should be partitioned by a column named `dt` (type string) extracted from `order_created_timestamp` in the format `YYYYMMDD`
    - Check `DESCRIBE TABLE` and `SAMPLE DATA` commands in the previous HIVE section.

3. After creating the above tables in Hive, create two new tables __order_detail_new__ and __restaurant_detail_new__ with their respective columns and partitions and add one new column for each table as explained below.
    - [X] `discount_no_null` - replace all the NULL values of discount column with 0
    - [X] `cooking_bin` - using esimated_cooking_time column and the below logic
    - Check `SAMPLE DATA` commands in the previous HIVE section.
    
    |estimated_cooking_time |cooking_bin|
    |-----  |--------   |
    |10-40  |1          |
    |41-80  |2          |
    |81-120 |3          |
    |> 120  |4          |

4. Final column count of each table (including partition column):

    - [X] order_detail = 9
    - [X] restaurant_detail = 7
    - [X] order_detail_new = 10
    - [X] restaurant_detail_new = 8 
    - Check `DESCRIBE TABLE` command in the previous HIVE section.

5. SQL requirements & CSV output requirements
    - [X] Get the average discount for each category
        - I'm not sure whether you need the average of `discount` or `discount_no_null` columns, so I calculate it both. It could lead to different business interpretation.
    - [X] Row count per each cooking_bin
    - Check the result in `./sql_result` for `discount.csv` and `cooking.csv`

# Technical Requirements
- [X] Use Apache Spark, Apache Sqoop or any other big data frameworks
- [X] Use a scheduler tool to run the pipeline daily. Airflow is preferred
- [X] Include a README file that explains how we can deploy your code
- [X] (bonus) Use Docker or Kubernetes for up-and-running program

## Dags

put dag image

# Question output
- [X] Source code 
- [X] Docker, docker-compose, kubernetes files if possible.
- [X] README of how to test / run




