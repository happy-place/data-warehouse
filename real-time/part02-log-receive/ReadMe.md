# 日志收集服务

服务管理脚本 logreceive

```bash
#!/bin/bash
JAVA_BIN=/opt/softwares/jdk1.8.0_144/bin/java
APPNAME=log-receive.jar
SRC_DIR=/opt/module/applog_reveiver/src
NGX=/opt/softwares/nginx-1.12.2/sbin/nginx
case $1 in
 "start")
   {
    for i in hadoop01 hadoop02 hadoop03
    do
     echo "start applog reveiver server[$i]..."
     ssh $i  "$JAVA_BIN -Xms32m -Xmx64m  -jar $SRC_DIR/$APPNAME >/dev/null 2>&1  &"
    done
    echo "start nginx[hadoop02]..."
    ssh hadoop02 sudo $NGX
  };;
  "stop")
  {
    echo "stop nginx[hadoop02]..."
    ssh hadoop02 sudo $NGX -s stop
    for i in  hadoop01 hadoop02 hadoop03
    do
     echo "stop applog reveiver server[$i]..."
     ssh $i "ps -ef | grep $APPNAME | grep -v grep|awk '{print \$2}' | sudo xargs kill  -s" > /dev/null 2>&1
    done
  };;
  "status")
  {
    echo "check nginx[hadoop02]..."
    ssh hadoop02 "ps -ef | grep nginx | grep -v grep | awk '{print \$2}'"

    for i in  hadoop01 hadoop02 hadoop03
    do
     echo "check applog reveiver server[$i]..."
     ssh $i "ps -ef | grep $APPNAME | grep -v grep | awk '{print \$2}'"
    done
  };;
  * )
  {
    echo 'logreceive (start | stop | status)'
  };;
  esac
```