hdfs dfs -rm -r /user/sqoop
hdfs dfs -mkdir /user/sqoop

/usr/lib/sqoop/bin/sqoop import \
--connect jdbc:postgresql://database:5432/lineman_wongnai \
--table order_detail \
--username postgres \
--password passw0rd \
--m 1 \
--target-dir /user/sqoop/order_detail \

/usr/lib/sqoop/bin/sqoop import \
--connect jdbc:postgresql://database:5432/lineman_wongnai \
--table restaurant_detail \
--username postgres \
--password passw0rd \
--m 1 \
--target-dir /user/sqoop/restaurant_detail \

# /usr/lib/sqoop/bin/sqoop import-all-tables \
# --connect jdbc:postgresql://database:5432/lineman_wongnai \
# --username postgres \
# --password passw0rd  \
# --num-mappers 1 \
# --hive-import \
# --hive-overwrite \
# --create-hive-table \
# --hive-database lineman_wongnai \
# --warehouse-dir /user/sqoop/lineman_wongnai