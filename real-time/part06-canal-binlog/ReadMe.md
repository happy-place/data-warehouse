# canal同步binlog

> 管理脚本 canal
> 
```bash
#!/bin/bash
SRC_DIR=/opt/softwares/canal
case $1 in
 "start")
   {
     echo "start canal[hadoop01] ..."
     $SRC_DIR/bin/startup.sh
  };;
  "stop")
  {
     echo "stop canal[hadoop01]..."
     ps -ef | grep CanalLauncher | grep -v grep | awk '{print $2}' | xargs kill -s
  };;
  "status")
  {
    echo "check maxwell[hadoop01]..."
    ps -ef | grep CanalLauncher | grep -v grep
  };;
  * )
  {
    echo 'canal (start | stop | status)'
  };;
esac
```