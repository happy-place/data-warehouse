package com.example.flink.app.dws

import com.alibaba.fastjson.JSONObject
import com.example.flink.bean.{KeywordStats, ProductStats, TestBean}
import com.example.flink.common.func.{IkAnalyzeUDTF, KeywordProductC2RUDTF}
import com.example.flink.util.{ClickhouseUtil, DebugUtil, KafkaUtil}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.util.Collector

/**
 * dws_product_stats
 * -> 注册动态表
 * -> 注册udtf函数(ik分词函数、指标散列)
 * -> 重新聚合
 * -> 抽取append流
 * -> 输出到clickhouse (dws_keyword_stats)
 *
 * 注：由于dws_product_stats中已经开过窗，因此数据可以直接使用，无需重新开窗
 */
object Keyword4ProductStatsApp {

  def main(args:Array[String]):Unit = {
    // todo 1 创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    env.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(120000)
    env.setStateBackend(new FsStateBackend("hdfs://hadoop01:9000/flink/checkpoint/gmall/keyword_4_product_stats_app"))
    env.setRestartStrategy(RestartStrategies.noRestart())

    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env,settings)

    val groupId = "keyword_4_product_stats_app_group"
    val productStatsTopic = "dws_product_stats"

    // todo 2 创建动态表
    val createSql = s"""CREATE TABLE product_stats (
                       |    spu_name STRING,
                       |    click_ct BIGINT,
                       |    cart_ct BIGINT,
                       |    order_ct BIGINT,
                       |	  stt STRING,
                       |    edt STRING
                       |) WITH (${KafkaUtil.getKafkaDDL(productStatsTopic,groupId)})
                       |""".stripMargin

    tableEnv.executeSql(createSql)

//    val testView = tableEnv.sqlQuery("select * from product_stats")
//    val testDS = tableEnv.toAppendStream[TestBean](testView)
//    testDS.print("test")
//
//    DebugUtil.interruptedException(testDS)
//
    // todo 3 注册UDTF
    tableEnv.createTemporaryFunction("ik_analyze",classOf[IkAnalyzeUDTF]) // 短语分词
    tableEnv.createTemporaryFunction("keywordProductC2R",classOf[KeywordProductC2RUDTF]) // 指标聚合

    // todo 4 依据分词
    val flatViewSql = s"""SELECT
         |  DATE_FORMAT(stt,'yyyy-MM-dd HH:mm:ss') stt,
         |  DATE_FORMAT(edt,'yyyy-MM-dd HH:mm:ss') edt,
         |  keyword,
         |  source,
         |  ct,
         |  UNIX_TIMESTAMP() * 1000 ts
         |FROM product_stats,
         |LATERAL TABLE(ik_analyze(spu_name)) AS T(keyword),
         |LATERAL TABLE(keywordProductC2R(click_ct ,cart_ct,order_ct)) AS T2(ct,source)
         |""".stripMargin

    val flatView = tableEnv.sqlQuery(flatViewSql)

    // todo 5 分组聚合spu_name炸开的keyword，存在重叠，因此需要再次聚合
    val aggViewSql = s"""SELECT stt,edt,keyword,source,sum(ct) as ct,ts
         |FROM ${flatView}
         |GROUP BY stt,edt,source,keyword,ts
         |""".stripMargin

    val aggView = tableEnv.sqlQuery(aggViewSql)

    // todo 6 抽取append流
    val productKeywordStatsDS = tableEnv.toAppendStream[KeywordStats](aggView)

    productKeywordStatsDS.print()

//    DebugUtil.interruptedException(productKeywordStatsDS)

    // todo 7 输出到clickhouse
    val sql = s"insert into dws_keyword_stats values (${("?"*6).mkString(",")})"
    productKeywordStatsDS.addSink(ClickhouseUtil.createSink[KeywordStats](sql,1000))

    env.execute("Keyword4ProductStatsApp Job")
  }

}
