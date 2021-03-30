package com.example.flink.app.dws

import com.example.flink.bean.KeywordStats
import com.example.flink.common.constant.GmallConstant
import com.example.flink.common.func.IkAnalyzeUDTF
import com.example.flink.util.{ClickhouseUtil, KafkaUtil}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.util.Collector

/**
 * 功能：自定义UDTF实现对页面日志中搜索内容关键词统计，即各关键字有多少点击
 * dwd_page_log
 * -> 注册动态表
 * -> 过滤出good_list页面item
 * -> 注册udft 分词函数
 * -> 对 item分词
 * -> 关键词统计（分组、开窗、聚合）
 * -> clickhouse (dws_keyword_stats)
 */

object KeywordStatsApp {

  def main(args:Array[String]):Unit = {
    // todo 1 创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    env.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(120000)
    env.setStateBackend(new FsStateBackend("hdfs://hadoop01:9000/flink/checkpoint/gmall/keyword_stats_app"))
    env.setRestartStrategy(RestartStrategies.noRestart())

    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    val groupId = "keyword_stats_app_group"
    val pageTopic = "dwd_page_log"

    // todo 2 注册动态表
    val createTable = s"""CREATE TABLE page_view (
                         |	  common MAP<STRING,STRING>,
                         |    page MAP<STRING,STRING>,ts BIGINT,
                         |    rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')) ,
                         |    WATERMARK FOR  rowtime  AS  rowtime - INTERVAL '2' SECOND
                         |) WITH (${KafkaUtil.getKafkaDDL(pageTopic,groupId)})
                         |""".stripMargin

    tableEnv.executeSql(createTable)

    // todo 3 过滤出 good_list 页面检索内容
    val fullwordViewSql = s"""SELECT page['item'] fullword, rowtime
                        |FROM page_view
                        |WHERE page['page_id'] = 'good_list' AND page['item'] IS NOT NULL
                        |""".stripMargin

    val fullwordView = tableEnv.sqlQuery(fullwordViewSql)

    // todo 4 注册分词检索 udtf
    tableEnv.createTemporaryFunction("ik_analyze",classOf[IkAnalyzeUDTF])

    // todo 5 使用分词检索udtf函数，对查询内容进行分词 （炸开）
    val keywordViewSql = s"""SELECT keyword,rowtime
         |FROM ${fullwordView},
         |LATERAL TABLE (ik_analyze(fullword)) as T(keyword)
         |""".stripMargin

    val keywordView = tableEnv.sqlQuery(keywordViewSql)

    // todo 6 对分词结果进行 分组、开窗、聚合
    val aggSql = s"""SELECT
                    |   DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt,
                    |   DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt,
                    |   keyword,
                    |   '${GmallConstant.KEYWORD_SEARCH}' source,
                    |   count(*) ct,
                    |   UNIX_TIMESTAMP() * 1000 ts
                    |FROM ${keywordView}
                    |GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND ), keyword
                    |""".stripMargin

    val keywordStatsTable = tableEnv.sqlQuery(aggSql)

    // todo 7 结果转换为append 流
    val keywordStatsDS = tableEnv.toAppendStream[KeywordStats](keywordStatsTable)

//    keywordStatsDS.print()

//    keywordStatsDS.process(
//      new ProcessFunction[KeywordStats,KeywordStats] {
//        override def processElement(i: KeywordStats, context: ProcessFunction[KeywordStats, KeywordStats]#Context,
//                                    collector: Collector[KeywordStats]): Unit = {
//          throw new RuntimeException("test")
//        }
//    })

    // todo 8 输出到 clickhouse
    val sql = s"insert into dws_keyword_stats values (${("?"*6).mkString(",")})"
    keywordStatsDS.addSink(ClickhouseUtil.createSink[KeywordStats](sql))

    // todo 9 启动执行
    env.execute("KeywordStatsApp Job")
  }

}
