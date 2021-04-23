from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

spark = SparkSession.builder.enableHiveSupport()\
.config("spark.sql.parquet.writeLegacyFormat",True)\
.getOrCreate()

df = spark.read.csv("hdfs://hive-namenode:8020/user/sqoop/order_detail/part-m-00000", header=False)
rename = {
    '_c0' : 'order_created_timestamp',
    '_c1' : 'status',
    '_c2' : 'price',
    '_c3' : 'discount',
    '_c4' : 'id',
    '_c5' : 'driver_id',
    '_c6' : 'user_id',
    '_c7' : 'restaurant_id',
}
df = df.toDF(*[rename[c] for c in df.columns])
df = df.withColumn('order_created_timestamp', F.to_timestamp('order_created_timestamp'))
df = df.withColumn('dt', F.date_format('order_created_timestamp', "yyyyMMdd"))
df = df.withColumn('price', F.col('price').cast(T.IntegerType()))
df = df.withColumn('discount', F.col('discount').cast(T.FloatType()))
df.write.parquet('hdfs://hive-namenode:8020/user/spark/transformed_order_detail', partitionBy='dt', mode='overwrite')

