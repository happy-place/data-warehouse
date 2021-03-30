package com.example.flink.app.dws

import com.example.flink.bean.ProvinceStats
import com.example.flink.util.{ClickhouseUtil, KafkaUtil}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment


/**
 * 功能：地区下单数量，成交金额统计
 * dwm_order_wide
 * -> 映射成动态表 (时间予以、水印)
 * -> 分组 开窗 聚合
 * -> table
 * -> append-only stream
 * -> clickhouse (dws_provience_stats)
 */

object ProvienceStatsApp {

  def main(args: Array[String]): Unit = {
    // todo 1 创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(120000)
    env.setStateBackend(new FsStateBackend("hdfs://hadoop01:9000/flink/checkpoint/gmall/provience_stats_app"))
    env.setRestartStrategy(RestartStrategies.noRestart())

    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    val groupId = "provience_stats_app_group"
    val orderWideTopic = "dwm_order_wide"

    // todo 2 注册动态表
    val createTable = s"""CREATE TABLE ORDER_WIDE (
      province_id BIGINT,
      province_name STRING,
      province_area_code STRING,
      province_iso_code STRING,
      province_3166_2_code STRING,
      order_id STRING,
      split_total_amount DECIMAL,
      create_time STRING,
      rowtime AS TO_TIMESTAMP(create_time),
      WATERMARK FOR rowtime AS rowtime
    ) WITH (${KafkaUtil.getKafkaDDL(orderWideTopic, groupId)})
    """.stripMargin

    tableEnv.executeSql(createTable)

    // todo 3 分组、开窗、聚合
    val table = tableEnv.sqlQuery(
      s"""SELECT
         |    DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') stt,
         |    DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') edt ,
         |    province_id,
         |	  province_name,
         |	  province_area_code area_code,
         |    province_iso_code iso_code ,
         |	  province_3166_2_code iso_3166_2 ,
         |   	SUM(split_total_amount) order_amount,
         |    COUNT( DISTINCT  order_id) order_count,
         |    UNIX_TIMESTAMP() * 1000 ts
         |FROM  ORDER_WIDE
         |GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND ),
         |province_id,
         |province_name,
         |province_area_code,
         |province_iso_code,
         |province_3166_2_code
         |""".stripMargin
    )

    // todo 4 转换为 append 流
    val provStatsDS = tableEnv.toAppendStream[ProvinceStats](table)

    // provStatsDS.print()

    // todo 5 输出
    val sql = s"INSERT INTO dws_province_stats VALUES (${("?" * 10).mkString(",")})"
    provStatsDS.addSink(ClickhouseUtil.createSink[ProvinceStats](sql))

    // todo 6 执行
    env.execute("ProvienceStatsApp Job")
  }

}
