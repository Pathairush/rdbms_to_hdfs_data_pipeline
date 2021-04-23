DROP TABLE IF EXISTS order_detail_new;

CREATE EXTERNAL TABLE IF NOT EXISTS order_detail_new (
    order_created_timestamp TIMESTAMP,
    status STRING,
    price INT,
    discount FLOAT,
    discount_no_null FLOAT,
    id STRING,
    driver_id STRING,
    user_id STRING,
    restaurant_id STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/user/spark/order_detail_new';

MSCK REPAIR TABLE order_detail_new;

SELECT COUNT(*) FROM order_detail_new;

SELECT * FROM order_detail_new LIMIT 5;

SELECT COUNT(*) FROM order_detail_new WHERE discount IS NULL;

SELECT COUNT(*) FROM order_detail_new WHERE discount_no_null IS NULL;

SHOW CREATE TABLE order_detail_new;