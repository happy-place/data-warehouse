# Dau日活统计

## 消息生产者
```aidl
0.ack应答机制，broker接收producer发送消息，然后给producer发送ack响应
    ack=0 -> producer只管写，
    ack=1 -> 有一个分区写成功就响应，如果刚好写成功的节点宕机，消息会丢失；
    
1.kafka事务机制保证生产消息不重复；
2.kafka自身partition基本为每条消息维护唯一SequenceNumber保证不重复，但是仅在当前kafka实例有效，一旦集群重启，就会变化。

结论：消息生产无法保证消息不重复（网络通信中重试机制导致、集群重启后Partition基本Seq变化。因此消息消费端去重势在必行。
```

## 消息消费
```aidl
方案1：借助redis去重，然后可以将明细数据写入其他位置，也可以不保存明细，直接将统计结果写入其他位置
1.借助redis set数据结构进行去重
2.去重后的明细数据保存到es
3.es上创建索引模板，以日期为单位管理每天数据；（幂等插入）
4.基于索引模板，在kibana上创建可视化面板，以<iframe>标签，移植到需要的地方；
5.通过elastic暴露数据接口，提供查询服务

方案2：不使用redis去重，明细直接写入幂等存储介质，通过幂等消除重复数据，此种情况必须保存明细，然后基于明细进行汇总

```

> spark任务父脚本
```shell
#!/bin/bash

JAVA_BIN=/opt/softwares/jdk1.8.0_144/bin/java
TITLE=${0##*/}
APPNAME=spark-job.jar
SRC_DIR=/opt/module/spark_job/src
LOG_DIR=/opt/module/spark_job/logs
case $1 in
 "start")
  {
     echo "start $TITLE [hadoop01]..."
     $JAVA_BIN -cp $SRC_DIR/$APPNAME -Dspark.testing.memory=1073741824 $MAIN > $LOG_DIR/${TITLE}.log 2>&1  &
  };;
  "stop")
  {
     echo "stop $TITLE [hadoop01]..."
     ps -ef | grep $MAIN | grep -v grep | awk '{print $2}' | xargs kill -s 9
  };;
  "status")
  {
     echo "check $TITLE [hadoop01]..."
     echo $MAIN
     ps -ef | grep $MAIN | grep -v grep
  };;
  * )
  {
    echo "$TITLE (start | stop | status)"
  };;
esac
```

## DB 转储 Kafka
> maxwell 数据分发脚本(ods -> dwd)，maxdist
```shell
#!/bin/bash
MAIN=com.example.log.ods.BaseDBMaxwellApp
source /opt/module/spark_job/bin/base.sh
```

> 用户维度表 转储phoenix，（user_dist）
```shell
#!/bin/bash
MAIN=com.example.log.dim.UserInfoApp
source /opt/module/spark_job/bin/base.sh
```

> 省份维度表 转储phoenix，（prov_dist）
```shell
MAIN=com.example.log.dim.ProvinceInfoApp
source /opt/module/spark_job/bin/base.sh
```

> 品牌维度，转储phoenix (trade_dist)
```shell
MAIN=com.example.log.dim.BaseTradeMarkApp
source /opt/module/spark_job/bin/base.sh
```

> 品类维度，转储phoenix (catetory3_dist)
```shell
MAIN=com.example.log.dim.BaseCatetory3App
source /opt/module/spark_job/bin/base.sh
```

> 商品SPU维度，转储phoenix (spu_dist)
```shell
MAIN=com.example.log.dim.SpuInfoApp
source /opt/module/spark_job/bin/base.sh
```

> 商品SKU维度退化表，转储phoenix (sku_dist)
```shell
MAIN=com.example.log.dim.SkuInfoApp
source /opt/module/spark_job/bin/base.sh
```

> order_detail 关联 sku 维度退化表
```shell
MAIN=com.example.log.dwd.OrderDetailApp
source /opt/module/spark_job/bin/base.sh
```

> order_info
```shell
MAIN=com.example.log.dwd.OrderInfoApp
source /opt/module/spark_job/bin/base.sh
```