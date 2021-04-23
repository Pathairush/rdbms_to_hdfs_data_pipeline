DROP TABLE IF EXISTS order_detail;

CREATE EXTERNAL TABLE IF NOT EXISTS order_detail (
    order_created_timestamp TIMESTAMP,
    status STRING,
    price INT,
    discount FLOAT,
    id STRING,
    driver_id STRING,
    user_id STRING,
    restaurant_id STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/user/spark/transformed_order_detail';

MSCK REPAIR TABLE order_detail;

SELECT COUNT(*) FROM order_detail;

SELECT * FROM order_detail LIMIT 5;

SHOW CREATE TABLE order_detail;