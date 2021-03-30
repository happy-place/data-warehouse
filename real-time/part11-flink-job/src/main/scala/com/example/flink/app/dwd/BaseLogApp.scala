package com.example.flink.app.dwd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.example.flink.util.KafkaUtil
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.util.Date

/**
 * 前端集中上报日志的topic，依据是页面日期、还是曝光日志、获取启动日期，分流到各自dwd层topic
 *
 * 启动日志：包含 start字段，从侧输出流输出到dwd_start_log
 * 非启动日志：不包含start字段，一律经过主流输出到dwd_page_log，且过滤出包含displays字段的消息，将每一条display消息带上page_id，输出到dwd_display_log
 *
 * 执行流程：
 * applog -> nginx -> log-receiver -> kafka -> flink(BaseLogApp) -> kafka (dwd_start_log、dwd_display_log、dwd_page_log)
 * BaseLogApp -> map 校正新用户标识(state)-> process 按启动日志、非启动日志、非启动日志中的曝光日志 -> kafka
 *
 * 前期准备：
 * logmock2、logreceiver、kafka、hadoop
 *
 */
object BaseLogApp {

  def main(args: Array[String]): Unit = {
    // todo 1 初始化环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度
    env.setParallelism(3)
    // 开启checkpoint，间隔5秒执行一次，消费kafka语义为精准一次
    env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE)
    // checkpoint超过60秒，放弃
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    // checkpoint目录
    env.setStateBackend(new FsStateBackend("hdfs://hadoop01:9000/flink/checkpoint/gmall/base_log_app"))

    env.setRestartStrategy(RestartStrategies.noRestart())

    val baseTopic = "ods_base_log"
    val groupId = "base_log_app"

    // todo 2 读取日志收集服务集中上报的kafka的topic，创建kafka stream
    val kafkaSource = KafkaUtil.createKafkaSource(baseTopic, groupId)
    val kafkaStream = env.addSource(kafkaSource)

    // 转换为对象流
    val objStream = kafkaStream.map(record => JSON.parseObject(record))

    // todo 3 修复 新老用户标识 （ETL）键控算子 + 状态
    // 接下来需要借助键控状态修复is_new标记，因此需要转换为keyedStream键控流
    val keyedStream = objStream.keyBy(_.getJSONObject("common").getString("mid"))

    // 修复 common 中 is_new 标记 ，为什么需要修复？用户退出登录后，重置注册，或登录，此时有可能被误判为新用户
    // is_new = "0" 为老顾客
    // is_new = "1" and 状态中不存在首次访问日期，为新顾客，此时需要保存本次访问日期到flink
    // is_new = "1" and 状态中存在首次访问日期，如果与当前消息日期相等，则已经为新顾客（首次访问同一天的状态都是新顾客），否则为老顾客，需要修改状态
    val fixedNewStateStream = keyedStream.map(new RichMapFunction[JSONObject, JSONObject] {
      // 监控状态，每个mid，对应一条记录(首次访问日期）
      var midFirstVistDateState: ValueState[String] = null
      var sdf: SimpleDateFormat = null

      // 富函数的声明周期函数，首次出现mid时执行一次，长用于进行初始化操作
      override def open(parameters: Configuration): Unit = {
        midFirstVistDateState = getRuntimeContext.getState(new ValueStateDescriptor("midFirstVistDateState", classOf[String]))
        sdf = new SimpleDateFormat("yyyyMMdd")
      }

      // 每次遇到相同mid执行一次
      override def map(in: JSONObject): JSONObject = {
        val isNew = in.getJSONObject("common").getString("is_new")
        if ("1".equals(isNew)) {
          val ts = in.getJSONObject("common").getLong("ts")
          val date = sdf.format(new Date(ts))
          val savedDate = midFirstVistDateState.value()
          if (savedDate != null && savedDate.size > 0) {
            if (!savedDate.equals(date)) {
              in.getJSONObject("common").put("is_new", "0")
            }
          } else {
            midFirstVistDateState.update(date)
          }
        }
        in
      }
    })

    // todo 4 processFunc 日志分类，启动日志和非启动日志中的曝光日志，借助侧输出流，输出到各自topic，另外所有非启动日志通过主流输出
    // 创建侧端输出标签
    val startTag = new OutputTag[String]("start_log")
    val displayTag = new OutputTag[String]("display_log")

    // 通过process api 执行分流处理逻辑
    // 启动日志：包含start字段的消息，输出到startTag所在侧端输出流，最后输出到dwd_start_log
    // 非启动日志：不包含start字段的消息，全部从主流输出，最后输出到dwd_page_log，
    //      且过滤出包含displays的字段，将每一条display 带上page_id 输出到displayTag所在侧端输出流
    val pageStream = fixedNewStateStream.process(new ProcessFunction[JSONObject, String] {
      override def processElement(i: JSONObject, context: ProcessFunction[JSONObject, String]#Context,
                                  collector: Collector[String]): Unit = {
        val start = i.getJSONObject("start")

        if (start != null && start.size() > 0) {
          context.output(startTag, i.toJSONString)
        }else {
          collector.collect(i.toJSONString)
          val displays = i.getJSONArray("displays")
          if(displays!=null && displays.size()>0){
            displays.forEach{any=>
              val display = any.asInstanceOf[JSONObject]
              val pageId = i.getJSONObject("page").getString("page_id")
              display.put("page_id",pageId)
              context.output(displayTag,display.toJSONString)
            }
          }
        }
      }
    })

    // 通过tag获取各自侧端输出流，并同主流一样输出到kafka
    val startLogStream = pageStream.getSideOutput(startTag)
    val displayLogStream = pageStream.getSideOutput(displayTag)

//    startLogStream.print("dwd_start_log")
//    displayLogStream.print("dwd_display_log")
//    pageStream.print("dwd_page_log")

    val startTopic = "dwd_start_log"
    val startSink = KafkaUtil.createKafkaSink(startTopic)
    startLogStream.addSink(startSink)

    val displayTopic = "dwd_display_log"
    val displaySink = KafkaUtil.createKafkaSink(displayTopic)
    displayLogStream.addSink(displaySink)

    val pageTopic = "dwd_page_log"
    val pageSink = KafkaUtil.createKafkaSink(pageTopic)
    pageStream.addSink(pageSink)

    // todo 5 启动计算
    env.execute("BaseLogApp Job")
  }

}
