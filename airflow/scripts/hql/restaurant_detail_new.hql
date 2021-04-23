DROP TABLE IF EXISTS restaurant_detail_new;

CREATE EXTERNAL TABLE IF NOT EXISTS restaurant_detail_new (
    id STRING,
    restaurant_name STRING,
    category STRING,
    estimated_cooking_time FLOAT,
    cooking_bin INT,
    latitude DECIMAL(11,8),
    longitude DECIMAL(11,8)
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/user/spark/restaurant_detail_new';

MSCK REPAIR TABLE restaurant_detail_new;

SELECT COUNT(*) FROM restaurant_detail_new;

SELECT * FROM restaurant_detail_new LIMIT 5;

SHOW CREATE TABLE restaurant_detail_new;