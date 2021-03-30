# phoenix
```aidl
hbase 开源sql皮肤，让用户很方便以jdbc连接方式操作hbase
```

```aidl
cd /opt/softwares

tar -zxvf apache-phoenix-4.14.2-HBase-1.3-bin.tar.gz -C .

mv apache-phoenix-4.14.2-HBase-1.3-bin phoenix-4.14.2-hbase-1.3

cd phoenix-4.14.2-hbase-1.3

cp phoenix-core-4.14.2-HBase-1.3.jar phoenix-4.14.2-HBase-1.3-client.jar  /opt/softwares/hbase-1.3.1/lib
cd /opt/softwares/hbase-1.3.1
xcall lib

ln -s /opt/softwares/hbase-1.3.1/conf/hbase-site.xml /opt/softwares/phoenix-4.14.2-hbase-1.3/bin
ln -s /opt/softwares/hadoop-2.7.2/etc/hadoop/core-site.xml /opt/softwares/phoenix-4.14.2-hbase-1.3/bin
ln -s /opt/softwares/hadoop-2.7.2/etc/hadoop/hdfs-site.xml /opt/softwares/phoenix-4.14.2-hbase-1.3/bin

hbaseStop
hbaseStart

vi ~/.bash_profile
----------------------------------------------------------------------------------------------------
export PHOENIX_HOME=/opt/softwares/phoenix-4.14.2-hbase-1.3
export PHOENIX_CLASSPATH=$PHOENIX_HOME
export PATH=$PATH:$PHOENIX_HOME/bin
----------------------------------------------------------------------------------------------------

source ~/.bash_profile

/opt/softwares/phoenix-4.14.2-hbase-1.3
bin/sqlline.py hadoop01,hadoop02,hadoop03:2181

0: jdbc:phoenix:hadoop01,hadoop02,hadoop03:21> !tables
+------------+--------------+-------------+---------------+----------+------------+----------------------------+-----------------+--------------+-----------------+---------------+---------------+-----------------+------------+-----------+
| TABLE_CAT  | TABLE_SCHEM  | TABLE_NAME  |  TABLE_TYPE   | REMARKS  | TYPE_NAME  | SELF_REFERENCING_COL_NAME  | REF_GENERATION  | INDEX_STATE  | IMMUTABLE_ROWS  | SALT_BUCKETS  | MULTI_TENANT  | VIEW_STATEMENT  | VIEW_TYPE  | INDEX_TYP |
+------------+--------------+-------------+---------------+----------+------------+----------------------------+-----------------+--------------+-----------------+---------------+---------------+-----------------+------------+-----------+
|            | SYSTEM       | CATALOG     | SYSTEM TABLE  |          |            |                            |                 |              | false           | null          | false         |                 |            |           |
|            | SYSTEM       | FUNCTION    | SYSTEM TABLE  |          |            |                            |                 |              | false           | null          | false         |                 |            |           |
|            | SYSTEM       | LOG         | SYSTEM TABLE  |          |            |                            |                 |              | true            | 32            | false         |                 |            |           |
|            | SYSTEM       | SEQUENCE    | SYSTEM TABLE  |          |            |                            |                 |              | false           | null          | false         |                 |            |           |
|            | SYSTEM       | STATS       | SYSTEM TABLE  |          |            |                            |                 |              | false           | null          | false         |                 |            |           |
+------------+--------------+-------------+---------------+----------+------------+----------------------------+-----------------+--------------+-----------------+---------------+---------------+-----------------+------------+-----------+
0: jdbc:phoenix:hadoop01,hadoop02,hadoop03:21> !quit

vi phoenix
----------------------------------------------------------------------------------------------------
#!/bin/bash

/opt/softwares/phoenix-4.14.2-hbase-1.3/bin/sqlline.py ${1:-hadoop01,hadoop02,hadoop03:2181}
----------------------------------------------------------------------------------------------------
```
IDEA 配置 Phoenix Driver
```aidl
驱动JAR：phoenix-4.14.2-HBase-1.3-client.jar
类：org.apache.phoenix.jdbc.PhoenixDriver
Advanced: 是jdk8编译

URL: jdbc:phoenix:hadoop01,hadoop02,hadoop03:2181 
```
hbase-site.xml
```aidl
<property>
       <name>phoenix.schema.isNamespaceMappingEnabled</name>
       <value>true</value>
</property>

<property>
      <name>phoenix.schema.mapSystemTablesToNamespace</name>
      <value>true</value>
</property>
```