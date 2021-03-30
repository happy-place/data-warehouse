# mock-db

> 模拟db数据：java -jar mock-db.jar --date=2021-03-06
> 启动脚本 dbmock

```bash
#!/bin/bash
JAVA_BIN=/opt/softwares/jdk1.8.0_144/bin/java
APPNAME=db-mock.jar
SRC_DIR=/opt/module/appdb_mock/src
BASE_DIR=`dirname $0`
case $1 in
 "start")
   {
     echo "start appdb mock ..."
     cd $SRC_DIR
     $JAVA_BIN -Xms32m -Xmx64m  -jar $APPNAME --date=${2:-`date +%Y-%m-%d`} >/dev/null 2>&1 &
     cd $BASE_DIR
  };;
  "stop")
  {
     echo "stop appdb mock ..."
     ps -ef | grep $APPNAME | grep -v grep | awk '{print $2}' | sudo xargs kill -s
  };;
  "status")
  {
     echo "check dbmock status"
     ps -ef | grep $APPNAME | grep -v grep
  };;
  * )
  {
    echo 'dbmock (start [yyyy-MM-dd]| stop | status)'
  }
esac
```