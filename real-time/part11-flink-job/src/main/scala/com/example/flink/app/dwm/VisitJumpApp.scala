package com.example.flink.app.dwm

import com.alibaba.fastjson.{JSON, JSONObject}
import com.example.flink.util.KafkaUtil
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.cep.{PatternFlatSelectFunction, PatternFlatTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.pattern.conditions.SimpleCondition
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.util
/**
 * 跳出:
 * 1)进入首个页面 last_page_id=null；
 * 2)10秒钟内没有跳转到其他页面。
 *
 * 基于dwd_page_log过滤出跳出日志存放到kafka(dwm_vist_jump)
 *
 * 前期准备：
 * applog -> nginx -> kafka -> BaseLogApp 分流 (dwd_page_log) -> VisitJumpApp 过滤出(dwm_visit_jump)
 *
 */
object VisitJumpApp {

  def main(args:Array[String]):Unit = {
    // todo 1 初始化环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    env.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.setStateBackend(new FsStateBackend("hdfs://hadoop01:9000/flink/checkpoint/visit_jump_app"))
    env.setRestartStrategy(RestartStrategies.noRestart())

    // todo 2 读取page流
    val groupId = "visit_jump_app_group"
    val sourceTopic = "dwd_page_log"
    val sinkTopic = "dwm_visit_jump"

    val kafkaSource = KafkaUtil.createKafkaSource(sourceTopic, groupId)
    val kafkaStream = env.addSource(kafkaSource)

    /*
        模拟kafka中的 dwd_page_log
        101 只有一条首个页面访问记录，因此判定为跳出记录
        102 首个页面访问时间与第二个页面访问时间差距13秒，超过规定10秒以内，因此也被判定位跳出记录
     */
//    val kafkaStream = env.fromElements(
//        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
//        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
//        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//          "\"home\"},\"ts\":25000} ",
//        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//          "\"detail\"},\"ts\":30000} "
//      )

    val objStream = kafkaStream.map(JSON.parseObject(_))

    // todo 3 指定时间语义、乱序水印
    // WatermarkStrategy.forMonotonousTimestamps[JSONObject] 默认是升序日志
    val timedStream = objStream.assignTimestampsAndWatermarks(
      WatermarkStrategy.forMonotonousTimestamps[JSONObject].withTimestampAssigner(
        new SerializableTimestampAssigner[JSONObject](){
          override def extractTimestamp(element: JSONObject, recordTimestamp: Long): Long = {
            element.getLong("ts")
          }
      })
    )

    // todo 4 转换为kv流，为接下来使用CEP进行用户访问习惯识别做准备
    val keyedByMidStream = timedStream.keyBy(_.getJSONObject("common").getString("mid"))

    // todo 5 CEP 过滤用户访问跳出记录(跳出实质是条件事件+超时事件)
    // todo 5.1 定义CEP 访问失败模型
    // 对符合条件的取反，就是跳出的 （访问首个页面后，10秒内页面没有变化的）
    val pattern = Pattern.begin("first")
                        .where( //过滤出 访问的首个页面
                          new SimpleCondition[JSONObject](){
                            override def filter(value: JSONObject): Boolean = {
                              val lastPageId = value.getJSONObject("page").getString("last_page_id")
                              if(lastPageId!=null && lastPageId.size>0){
                                false
                              }else{
                                true
                              }
                            }
                          }
                        )
                        .next("next")
                        .where( //过滤出 紧随首个页面之后访问页面
                          new SimpleCondition[JSONObject](){
                            override def filter(value: JSONObject): Boolean = {
                              val lastPageId = value.getJSONObject("page").getString("last_page_id")
                              if(lastPageId!=null && lastPageId.size>0){
                                true
                              }else{
                                false
                              }
                            }
                          }
                        ).within(Time.milliseconds(10000)) // 要求在10秒内完成访问


    // todo 5.2 使用定义好的cep模型，过滤用户数据，将数据划分为超时记录和正常匹配记录
    val patternStream = CEP.pattern(keyedByMidStream, pattern)

    val jumpTag = new OutputTag[JSONObject]("jump_tag")

    // todo 5.3 从cep流中取出超时事件 和 正常事件，超时时间即为与模型匹配事件，通过侧输出流输出到kafka，正常事件，不做处理
    val noJumpStream = patternStream.flatSelect(jumpTag,
      new PatternFlatTimeoutFunction[JSONObject,JSONObject](){
        // 超时的流
        override def timeout(map: util.Map[String, util.List[JSONObject]], l: Long, collector: Collector[JSONObject]): Unit = {
          val objs = map.get("first")
          objs.forEach(collector.collect(_)) // 收集到 jumpTag
        }
      },
      new PatternFlatSelectFunction[JSONObject,JSONObject](){
        // 符合CEP规则，未超时的流，由于不需要，因此不做任何处理
        override def flatSelect(map: util.Map[String, util.List[JSONObject]], collector: Collector[JSONObject]): Unit = {
          // 不做任何处理
        }
      })

    val jumpStream = noJumpStream.getSideOutput(jumpTag).map(_.toJSONString)

    jumpStream.print("junm visit")

    val kafkaSink = KafkaUtil.createKafkaSink(sinkTopic)
    jumpStream.addSink(kafkaSink)

    // todo 6 启动计算
    env.execute("VistJumpApp Job")
  }


}
