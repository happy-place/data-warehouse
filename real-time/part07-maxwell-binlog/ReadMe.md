# maxwell 同步binlog

> 服务管理脚本 max
> 
```bash
#!/bin/bash
JAVA_BIN=/opt/softwares/jdk1.8.0_144/bin/java
APPNAME=spark-job.jar
SRC_DIR=/opt/module/spark_job/src
MAIN=com.example.log.ods.BaseDBMaxwellApp
case $1 in
 "start")
  {
     echo "start maxwell dist[hadoop01]..."
     $JAVA_BIN -cp $SRC_DIR/$APPNAME -Dspark.testing.memory=1073741824 $MAIN > /dev/null 2>&1  &
  };;
  "stop")
  {
     echo "stop maxwell dist[hadoop01]..."
     ps -ef | grep $MAIN | grep -v grep | awk '{print $2}' | xargs kill -s 9
  };;
  "status")
  {
     echo "check maxwell dist[hadoop01]..."
     ps -ef | grep $MAIN | grep -v grep
  };;
  * )
  {
    echo 'max_dist (start | stop | status)'
  };;
  esac

```