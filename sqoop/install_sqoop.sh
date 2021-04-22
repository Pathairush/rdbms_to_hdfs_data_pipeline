cd /opt/sqoop
curl -o /opt/sqoop/sqoop.tar.gz https://downloads.apache.org/sqoop/1.4.7/sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz
tar -xf /opt/sqoop/sqoop.tar.gz
mv /opt/sqoop/sqoop-1.4.7.bin__hadoop-2.6.0 /usr/lib/sqoop
rm -r sqoop-1.4.7.bin__hadoop-2.6.0
export SQOOP_HOME=/usr/lib/sqoop 
export PATH=$PATH:$SQOOP_HOME/bin
source ~/.bashrc
cp /opt/sqoop/sqoop-env.sh /usr/lib/sqoop/conf/sqoop-env.sh
cp /opt/sqoop/postgresql-42.2.19.jre6.jar /usr/lib/sqoop/lib