package com.example.log.dau

import com.alibaba.fastjson.JSON
import com.example.log.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util.Date

object C2AddTime {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("dau_task").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val topic = "rt_start_log"
    val groupId = "dau_group"
    val kafkaStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)

    val objectStream = kafkaStream.map {
      case cr: ConsumerRecord[String, String] => {
        val nObject = JSON.parseObject(cr.value())
        val ts = nObject.getString("ts").toLong
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val date = new Date(ts)
        val dateStr = sdf.format(date)
        val arr = dateStr.split(" ")
        nObject.put("dt", arr(0))
        nObject.put("hr", arr(1).substring(0, 2))
        nObject
      }
    }

    objectStream.print(200)
    ssc.start()
    ssc.awaitTermination()
  }

}
