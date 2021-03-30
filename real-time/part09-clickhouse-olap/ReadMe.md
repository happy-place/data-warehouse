# clickhouse

> 管理脚本 click
```shell
#!/bin/bash

TITLE=${0##*/}
case $1 in
 "start")
  {
     echo "start $TITLE [hadoop03]..."
     ssh hadoop03 "sudo systemctl start clickhouse-server"
  };;
 "start_cluster")
  {
     echo "start $TITLE [cluster]..."
     xcall sudo systemctl start clickhouse-server
  };;
  "stop")
  {
     echo "stop $TITLE [hadoop03]..."
     ssh hadoop03 "sudo systemctl stop clickhouse-server"
  };;
 "stop_cluster")
  {
     echo "stop $TITLE [cluster]..."
     xcall sudo systemctl stop clickhouse-server
  };;
  "status")
  {
     echo "check $TITLE [hadoop03]..."
     ssh hadoop03 "sudo systemctl status clickhouse-server"
  };;
 "status_cluster")
  {
     echo "check $TITLE [cluster]..."
     xcall sudo systemctl status clickhouse-server
  };;
  "restart")
  {
     echo "restart $TITLE [hadoop03]..."
     ssh hadoop03 "sudo systemctl restart clickhouse-server"
  };;
 "restart_cluster")
  {
     echo "restart $TITLE [cluster]..."
     xcall sudo systemctl restart clickhouse-server
  };;
  "client")
  {
     echo "conn $TITLE [hadoop03]..."
     clickhouse-client -h hadoop03 -m
  };;
  * )
  {
    echo "$TITLE (start | start_cluster | restart | restart_cluster | stop | stop_cluster | status | status_cluster | client)"
  };;
esac
```