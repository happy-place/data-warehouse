package com.example.log.dau

import com.alibaba.fastjson.JSON
import com.example.log.model.DauInfo
import com.example.log.util.{MyESUtil, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util.Date

object DauApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val topic = "rt_start_log"
    val groupId = "dau_app_group" // group 名称尽量与类名一致

    // step1: 获取自己维护的offset信息
    val currentOffsets = OffsetManagerUtil.getOffsetFromRedis(topic, groupId)

    // step2: 如果是首次消费，直接创建kafka流，否则需要定位读取
    var kafkaStream: InputDStream[ConsumerRecord[String, String]] = null
    if (currentOffsets != null && currentOffsets.size > 0) {
      kafkaStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId, currentOffsets)
    } else {
      kafkaStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    // step3: 提前保存好当前流的偏移量信息，这里是transform 会触发计算
    var offsetRanges = Array.empty[OffsetRange]
    val offsetDStream = kafkaStream.transform { rdd =>
      println(rdd.getClass)// KafkaRDD ,混入了 HasOffsetRanges,因此可以进行强转
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach { or =>
        println(s"partition: ${or.partition} offset: ${or.fromOffset} => ${or.untilOffset}")
      }
      rdd
    }

    // step4: 添加新字段（dt、hr、mi）
    val objStream = offsetDStream.map { record =>
      val obj = JSON.parseObject(record.value())
      val ts = obj.getString("ts").toLong
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val arr = sdf.format(new Date(ts)).split(" ")
      val dt = arr(0)
      val hr = arr(1).substring(0, 2)
      val mi = arr(1).substring(3, 5)
      obj.put("dt", dt)
      obj.put("hr", hr)
      obj.put("mi", mi)
      obj
    }

    // step4:借助redis去重，过滤出首条启动日志
    val filtedStream = objStream.filter { obj =>
      val client = MyRedisUtil.getJedisClient
      val dt = obj.getString("dt")
      val key = s"dau:${dt}"
      val mid = obj.getJSONObject("common").getString("mid")
      val isNew = client.sadd(key, mid) == 1L
      client.expire(key, 24 * 3600)
      client.close()
      isNew
    }

    // step4: 去重后的信息保存到es，保存完毕标志着信息处理完毕，需要将前面已经提前的offsetRanges自己保存维护
    filtedStream.foreachRDD { rdd =>
      val client = MyESUtil.getClient
      rdd.foreachPartition { iter =>
        val dauMap = iter.map { obj =>
          val dt = obj.getString("dt")
          val index = s"gmall${dt.substring(0, 4)}_dau_info_${dt}"
          val commonObj = obj.getJSONObject("common")
          val mid = commonObj.getString("mid")
          val key = s"${mid}:${index}"
          val dauInfo = DauInfo(
            mid,
            commonObj.getString("uid"),
            commonObj.getString("ar"),
            commonObj.getString("ch"),
            commonObj.getString("vc"),
            obj.getString("dt"),
            obj.getString("hr"),
            obj.getString("mi"),
            obj.getString("ts").toLong
          )
          (key, dauInfo)
        }.toMap
        val result = MyESUtil.bulkInsert(dauMap)
        val failedItems = result.getFailedItems()
        if (failedItems.size() > 0) {
          println(s"${failedItems.size()}条记录保持到ES失败")
        }
      }
      client.close()
      OffsetManagerUtil.saveOffsetToRedis(topic, groupId, offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
