hdfs dfs -rm -r /user/sqoop
hdfs dfs -mkdir /user/sqoop

/usr/lib/sqoop/bin/sqoop import \
--connect jdbc:postgresql://database:5432/lineman_wongnai \
--table order_detail \
--username postgres \
--password passw0rd \
--num-mappers 1 \
--map-column-hive order_created_timestamp=STRING,status=STRING,price=INT,discount=FLOAT,id=STRING,driver_id=STRING,user_id=STRING,restaurant_id=STRING \
--target-dir /user/sqoop/order_detail \

/usr/lib/sqoop/bin/sqoop import \
--connect jdbc:postgresql://database:5432/lineman_wongnai \
--table restaurant_detail \
--username postgres \
--password passw0rd \
--num-mappers 1 \
--target-dir /user/sqoop/restaurant_detail \