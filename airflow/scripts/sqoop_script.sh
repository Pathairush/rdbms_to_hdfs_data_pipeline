hdfs dfs -mkdir /user/sqoop
sqoop import --connect jdbc:postgresql://database:5432/lineman_wongnai --table order_detail --username postgres --password passw0rd --m 1 --target-dir /user/sqoop/order_detail
sqoop import --connect jdbc:postgresql://database:5432/lineman_wongnai --table restaurant_detail --username postgres --password passw0rd --m 1 --target-dir /user/sqoop/restaurant_detail