# applog 日志模拟

> 模拟db数据：java -jar mock-log.jar --date=2021-03-06 

> 启动脚本 logmock

```bash
#!/bin/bash
JAVA_BIN=/opt/softwares/jdk1.8.0_144/bin/java
APPNAME=log-mock.jar
SRC_DIR=/opt/module/applog_mock/src
BASE_DIR=`dirname $0`
case $1 in
 "start")
   {
     echo "start applog mock ..."
     cd $SRC_DIR
     $JAVA_BIN -Xms32m -Xmx64m  -jar $APPNAME --date=${2:-`date +%Y-%m-%d`} >/dev/null 2>&1  &
     cd $BASE_DIR
  };;
  "stop")
  {
     echo "stop applog mock ..."
     ps -ef | grep $APPNAME | grep -v grep | awk '{print $2}' | sudo xargs kill -s
  };;
  "status")
  {
     echo "check logmock status"
     ps -ef | grep $APPNAME | grep -v grep
  };;
  * )
  {
    echo 'logmock (start [yyyy-MM-dd]| stop | status)'
  }
esac
```