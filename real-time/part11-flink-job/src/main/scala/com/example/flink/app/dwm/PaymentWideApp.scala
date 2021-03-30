package com.example.flink.app.dwm

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.{SerializeConfig, SerializerFeature}
import com.example.flink.bean.{OrderWide, PaymentInfo, PaymentWide}
import com.example.flink.util.KafkaUtil
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.time.Duration

/**
 * 功能：订单支付流上补充商品明细
 * 执行流程：
 * dwm_order_wide interval join dwd_payment_info -> dwm_payment_wide
 *
 * 前期准备：
 * maxwell kafka db_dist OrderWideApp
 *
 */
object PaymentWideApp {

  def main(args:Array[String]):Unit = {
    // todo 1 初始化环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    env.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(120000)
    env.setStateBackend(new FsStateBackend("hdfs://hadoop01:9000/flink/checkpoint/gmall/payment_wide_app"))
    env.setRestartStrategy(RestartStrategies.noRestart())

    val groupId = "payment_wide_app_group"
    val orderWideTopic = "dwm_order_wide"
    val paymentInfoTopic = "dwd_payment_info"
    val paymentWideTopic = "dwm_payment_wide"

    val orderWideSource = KafkaUtil.createKafkaSource(orderWideTopic, groupId)
    val paymentInfoSource = KafkaUtil.createKafkaSource(paymentInfoTopic, groupId)

    // todo 2 创建订单、支付流
    val orderWideDS = env.addSource(orderWideSource).map(JSON.parseObject(_,classOf[OrderWide]))
    val paymentInfoDS = env.addSource(paymentInfoSource).map{
      new RichMapFunction[String,PaymentInfo](){
        private var sdf:SimpleDateFormat = null

        override def open(parameters: Configuration): Unit = {
          sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        }

        override def map(value: String): PaymentInfo = {
          val info = JSON.parseObject(value, classOf[PaymentInfo])
          info.create_ts = sdf.parse(info.create_time).getTime
          info
        }
      }
    }

//    orderWideDS.print("order_wide")
//    paymentInfoDS.print("payment_info")

    // todo 3 指定双流的时间语义、乱序水印
    val orderWideWithTsDS = orderWideDS.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(
        new SerializableTimestampAssigner[OrderWide](){
          override def extractTimestamp(element: OrderWide, recordTimestamp: Long): Long = element.create_ts
        }
      ))

    val paymentInfoWithTsDS = paymentInfoDS.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(
        new SerializableTimestampAssigner[PaymentInfo](){
          override def extractTimestamp(element: PaymentInfo, recordTimestamp: Long): Long = element.create_ts
        }
      ))

    // todo 4 声明双流各自的key
    val keyedOrderWideDS = orderWideWithTsDS.keyBy(_.order_id)
    val keyedPaymentInfoDS = paymentInfoWithTsDS.keyBy(_.order_id)

    // todo 5 interval join 拼接双流
    val paymentWideDS = keyedPaymentInfoDS.intervalJoin[OrderWide](keyedOrderWideDS)
      .between(Time.seconds(-20),Time.seconds(0))
      .process(new ProcessJoinFunction[PaymentInfo,OrderWide,PaymentWide](){
        override def processElement(paymentInfo: PaymentInfo, orderWide: OrderWide,
                                    context: ProcessJoinFunction[PaymentInfo, OrderWide, PaymentWide]#Context,
                                    collector: Collector[PaymentWide]): Unit = {
          val paymentWide = new PaymentWide(paymentInfo, orderWide)
          collector.collect(paymentWide)
        }
      }).map(JSON.toJSONString(_,new SerializeConfig(true),SerializerFeature.WriteMapNullValue))

//    paymentWideDS.print("payment_wide")

    // todo 6 订单支付宽表输出到kafka
    val paymentWideSink = KafkaUtil.createKafkaSink(paymentWideTopic)
    paymentWideDS.addSink(paymentWideSink)

    // todo 7 启动计算
    env.execute("PaymentWideApp Job")
  }

}
