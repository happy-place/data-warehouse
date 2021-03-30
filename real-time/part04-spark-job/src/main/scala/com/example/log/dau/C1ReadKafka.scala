package com.example.log.dau

import com.example.log.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object C1ReadKafka {

  // 从kafka读取消息
  def main(args: Array[String]): Unit = {
    // app 名称，使用线程数
    val conf = new SparkConf().setAppName("DauJob").setMaster("local[2]")
    // 每5秒一个批次
    val ssc = new StreamingContext(conf, Seconds(5))

    val groupId = "dau_group"
    val topic = "rt_start_log"

    // InputDStream[ConsumerRecord[String, String]]
    val kafkaStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)

    // 上面得到的流不能直接输出，需要抓取value才能打印输出
    kafkaStream.map(_.value()).print(200)
    ssc.start()
    ssc.awaitTermination()
  }

}
