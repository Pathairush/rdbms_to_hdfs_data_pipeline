from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

spark = SparkSession.builder.enableHiveSupport()\
.config("spark.sql.parquet.writeLegacyFormat",True)\
.getOrCreate()

order = spark.read.parquet("hdfs://hive-namenode:8020/user/spark/transformed_order_detail")
restaurant = spark.read.parquet("hdfs://hive-namenode:8020/user/spark/transformed_restaurant_detail")
order_new = spark.read.parquet("hdfs://hive-namenode:8020/user/spark/order_detail_new")
restaurant_new = spark.read.parquet("hdfs://hive-namenode:8020/user/spark/restaurant_detail_new")

def count_row(sdf):
    return sdf.count()

count_row_dict = {
    'order' : [order, 395361],
    'order_detail_new' : [order_new, 395361],
    'restaurant' : [restaurant, 12623],
    'restaurant_detail_new' : [restaurant_new, 12623],
}

fail_cases = list()
for dataset, items in count_row_dict.items():
    df = items[0]
    expected_result = items[1]
    if count_row(df) == expected_result:
        continue
    else:
        fail_cases.append(dataset)

if fail_cases:
    raise ValueError(f'the number of row is not equal to expected output : {fail_cases}')
else:
    print("-- SPARK DQ CHECK PASSED")