from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

spark = SparkSession.builder.enableHiveSupport()\
.config("spark.sql.parquet.writeLegacyFormat",True)\
.getOrCreate()

df = spark.read.parquet("hdfs://hive-namenode:8020/user/spark/transformed_order_detail")
df = df.withColumn('discount_no_null', F.col('discount'))
df = df.na.fill({'discount_no_null':0.0})
df.write.parquet('hdfs://hive-namenode:8020/user/spark/order_detail_new', partitionBy='dt', mode='overwrite')