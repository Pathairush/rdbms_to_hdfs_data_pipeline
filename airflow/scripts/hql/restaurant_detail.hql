DROP TABLE IF EXISTS restaurant_detail;

CREATE EXTERNAL TABLE IF NOT EXISTS restaurant_detail (
    id STRING,
    restaurant_name STRING,
    category STRING,
    estimated_cooking_time FLOAT,
    latitude DECIMAL(11,8),
    longitude DECIMAL(11,8)
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/user/spark/transformed_restaurant_detail';

MSCK REPAIR TABLE restaurant_detail;

SELECT COUNT(*) FROM restaurant_detail;

SELECT * FROM restaurant_detail LIMIT 5;

SHOW CREATE TABLE restaurant_detail;