package com.example.flink.util

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import org.junit.Test

/**
 * flink重启策略
 * 1.未开启checkpoint时，等同于noRestart，即出现异常立即崩溃，不重启
 * 2.开启checkpoint，但不指定RestartStrategies时，默认重启Integer.maxValue()，即无限重启;
 * 3.开启checkpoint，指定RestartStragegies时，按指定重启策略重启，
 * RestartStrategies.noRestart() 无重启
 * RestartStrategies.failureRateRestart() 满足一定失败率重启
 * RestartStrategies.fixedDelayRestart() 固定间隔重启
 * RestartStrategies.fallBackRestart() 回调重启
 *
 */
class RestartTest {

  @Test
  def test1(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.setStateBackend(new FsStateBackend("hdfs://hadoop01:9000/flink/checkpoint/gmall/test"))
    //
    //    RestartStrategies.noRestart() 无重启
    //    RestartStrategies.failureRateRestart() 满足一定失败率重启
    //    RestartStrategies.fixedDelayRestart() 固定间隔重启
    //    RestartStrategies.fallBackRestart() 回调重启
    env.setRestartStrategy(RestartStrategies.noRestart())

    val sourceTopic = "ods_base_db"
    val groupId = "base_db_app_group"

    val kafkaSource = KafkaUtil.createKafkaSource(sourceTopic, groupId)
    val kafkaStream = env.addSource(kafkaSource)

    kafkaStream.print("转换前>>>")

    val jsonStream = kafkaStream.map { record =>
      val num = 5 / 0 // by zero 异常
      JSON.parseObject(record)
    }

    jsonStream.print("转换后>>>")

    env.execute("BaseDBApp Job")
  }


}
