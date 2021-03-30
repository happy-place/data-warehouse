package com.example.flink.app.dwd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.example.flink.common.func.{KafkaSinkSerializeSchema, PhoenixSinkFunc, TableProcessFunc}
import com.example.flink.util.KafkaUtil
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

/**
 * maxwell采集到kafka的业务数据，依据是事实还是维度相关，拆分到kafka和hbase
 *
 * binlog 集中收集的topic，已经是维度还是事实分流到hbase或kafka
 *
 * 执行流程:
 *  服务端 ddl/dml -> mysql -> binlog -> canal\maxwell -> kafka (ods_base_db) -> flink(BaseDBApp) -> hbase(dim_xxx) | kafka (dwd_xxx)
 *  BaseDBApp ->  filter ETL清洗、过滤不合法数据 -> process 查 mysql(table_process) 参照配置执行分流，维度数据写入hbase，事实数据写入kafka -> sink
 *
 * 启动前期准备：
 *  dbmock2 mysql 、maxwell、kafka、hadoop、hbase
 *
 * 注：维度数据，如果是update操作，需要配合更新或清除redis相关缓存
 */
object BaseDBApp {

  def main(args:Array[String]):Unit = {
    // TODO 1 初始化环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    env.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.setStateBackend(new FsStateBackend("hdfs://hadoop01:9000/flink/checkpoint/gmall/base_db_app"))

    env.setRestartStrategy(RestartStrategies.noRestart())

    val sourceTopic = "ods_base_db"
    val groupId = "base_db_app_group"

    // TODO 2 接入binlog集中上报kafka流
    val kafkaSource = KafkaUtil.createKafkaSource(sourceTopic, groupId)
    val kafkaStream = env.addSource(kafkaSource)

    // TODO 3 对流进行建模，清洗转换、过滤 ETL
    val jsonStream = kafkaStream.map(JSON.parseObject(_))

    // ETL 清洗、非空判断
    val filteredStream = jsonStream.filter{json=>
      json.getString("table")!=null &&
        json.getJSONObject("data")!=null &&
        json.getString("data").size > 3
    }

    // TODO 4 借助processFunc对业务数据进行动态分流，维度数据通过侧输出，写入hbase，事实数据保留在主流中，依据业务各自kafka中的topic
    val dimTag = new OutputTag[JSONObject]("dim_tag")

    val factStream = filteredStream.process(new TableProcessFunc(dimTag))

    val dimStream = factStream.getSideOutput(dimTag)

//    dimStream.print("dim hbase")
//    factStream.print("fact kafka")

    dimStream.addSink(new PhoenixSinkFunc())
//    factStream.addSink(new KafkaSinkFunc())
    factStream.addSink(KafkaUtil.createKafkaSinkBySchema(new KafkaSinkSerializeSchema))
    // TODO 5 启动计算
    env.execute("BaseDBApp Job")
  }


}
