DROP TABLE IF EXISTS order_detail;

-- HIVE CANNOT READ TIMESTAMP FOR PARQUET FILE FORMAT -> https://issues.apache.org/jira/browse/HIVE-15079
-- WORKAROUND HERE -> https://stackoverflow.com/questions/60492836/timestamp-not-behaving-as-intended-with-parquet-in-hive 
CREATE EXTERNAL TABLE IF NOT EXISTS order_detail (
    order_created_timestamp STRING,
    status STRING,
    price INT,
    discount DOUBLE,
    id STRING,
    driver_id STRING,
    user_id STRING,
    restaurant_id STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET
LOCATION '/user/sqoop/order_detail';

SELECT COUNT(*) FROM order_detail;