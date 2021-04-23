from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

spark = SparkSession.builder.enableHiveSupport()\
.config("spark.sql.parquet.writeLegacyFormat",True)\
.getOrCreate()

df = spark.read.parquet("hdfs://hive-namenode:8020/user/spark/transformed_restaurant_detail")
df = df.withColumn("cooking_bin",
                    F.when( F.col('estimated_cooking_time').between(10,40), 1 )\
                    .otherwise( F.when(F.col('estimated_cooking_time').between(41,80), 2 )\
                        .otherwise( F.when( F.col('estimated_cooking_time').between(81,120), 3 )\
                            .otherwise( F.when(F.col('estimated_cooking_time') > 120, 4 )\
                                .otherwise(F.lit(None))
                                )
                            )
                        )
                    )
df.write.parquet('hdfs://hive-namenode:8020/user/spark/restaurant_detail_new', partitionBy='dt', mode='overwrite')