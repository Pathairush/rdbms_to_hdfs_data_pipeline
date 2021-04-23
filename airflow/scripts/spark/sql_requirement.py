from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import os
import shutil

spark = SparkSession.builder.enableHiveSupport()\
.config("spark.sql.parquet.writeLegacyFormat",True)\
.getOrCreate()

order_detail_new = spark.read.parquet("hdfs://hive-namenode:8020/user/spark/order_detail_new")
restaurant_detail_new = spark.read.parquet("hdfs://hive-namenode:8020/user/spark/restaurant_detail_new")

df = order_detail_new.join(restaurant_detail_new, on = order_detail_new.restaurant_id == restaurant_detail_new.id, how='inner')

avg_discount_category = df.groupby('category').agg(F.avg('discount'), F.avg('discount_no_null'))
avg_discount_category.show()
avg_discount_category.coalesce(1).write.csv('/home/sql_result/discount', header=True, mode='overwrite')

row_count_cooking_bin = df.groupby('cooking_bin').count()
row_count_cooking_bin.show()
row_count_cooking_bin.coalesce(1).write.csv('/home/sql_result/cooking', header=True, mode='overwrite')

def rename_csv_from_spark(spark_csv_path, output_filename):
    fname = os.path.join(spark_csv_path, [f for f in os.listdir(spark_csv_path) if f.endswith('csv')][0])
    os.rename(fname, f'/home/sql_result/{output_filename}')
    shutil.rmtree(spark_csv_path)

rename_csv_from_spark('/home/sql_result/discount', 'discount.csv')
rename_csv_from_spark('/home/sql_result/cooking', 'cooking.csv')
