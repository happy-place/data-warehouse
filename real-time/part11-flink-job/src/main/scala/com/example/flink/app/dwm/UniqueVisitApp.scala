package com.example.flink.app.dwm

import com.alibaba.fastjson.{JSON, JSONObject}
import com.example.flink.common.func.FirstPageFilterFunc
import com.example.flink.util.KafkaUtil
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._


/**
 * 基于dwd_page_log计算独立访问用户数
 * 1.只有用户进入的首个页面的日志参与统计，last_page_id==null判定为用户进入的首个页面;
 * 2.只有首次访问记录才参与统计，依据键控状态去重
 *
 * ODS 承接log和binlog，一般都是集中上报到一个topic
 * DWD 对ods层中事实数据进行ETL清洗、保留需要字段，结合数据模型，写出到kafka不同topic，有业务模式概念
 * DIM 对ods层中维度数据进行ETL清洗，只保留insert、update，保留需要字段，结合数据模型，写出到hbase，有业务模型概念，此外维度直接为方便使用，
 * 可以先进行维度退化（join成维度宽表），方便后期使用；
 * STATE 基于ODS层中事实表，产生的衍生维度，如：新用户标识，下单标识等，与维度不同在于，维度只跟随DB变化，而状态是根据事实数据渐变，
 *  通常可以维护在flink的状态算子，或维护在外部如hbase等存储介质。
 * DWM 数据进入DWS汇总前，需要join一些维度，会执行去重操作distinct，介于DWD明细层和DWS汇总层直接的过渡层，数据写回kafka
 * DWS 指标汇总层，通常执行group by汇总，通常以宽表形式，输出到olap存储介质，如：clickhouse、hive、presto、mysql、impala
 * ADS DWS层数据，以接口形式暴露给UI（echart），或直接基于ES、tableau、DataV进行可视化展示。
 *
 * 执行流程：
 * kafka(dwd_start_log) -> filter -> kafka (dwm_unique_vist)
 * filter: last_page_id == null 访问首个页面 && (visitDateState == null 无状态 || savedDate != logDate 跨天)
 *
 * 前期准备：
 * logmock2 nginx logreceiver kafka hadoop BaseLogApp
 *
 *
 */
object UniqueVisitApp {

  def main(args:Array[String]):Unit = {
    // todo 1 初始化环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 测试使用 默认http://localhost:8081，端口如果被占用，随机变更
//    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration())
    env.setParallelism(3)
    env.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.setRestartStrategy(RestartStrategies.noRestart())
    env.setStateBackend(new FsStateBackend("hdfs://hadoop01:9000/flink/checkpoint/gmall/unique_visit_app"))

    // todo 2 创建page流
    val groupId = "unique_visit_app_group"
    val sourceTopic = "dwd_page_log"
    val sinkTopic = "dwm_unique_visit"
    val kafkaSource = KafkaUtil.createKafkaSource(sourceTopic,groupId)

    val kafkaStream = env.addSource(kafkaSource)

    val objStream = kafkaStream.map(JSON.parseObject(_))
    // todo 3 指定流的键，为键控状态过滤做准备
    val midKeyedStream = objStream.keyBy(_.getJSONObject("common").getString("mid"))

    // todo 4 RichFilterFunc过滤出当日首个访问页面的流
    val firstPageStream = midKeyedStream.filter(new FirstPageFilterFunc).map(_.toJSONString)

    // page字段中没有last_page_id字段
//    firstPageStream.print()

    // todo 5 输出到kafka，为之后uv统计做准备
    val kafkaSink = KafkaUtil.createKafkaSink(sinkTopic)

//    firstPageStream.print()

    firstPageStream.addSink(kafkaSink)

    // todo 6 启动计算
    env.execute("UniqueVisitApp Job")
  }

}
