package com.example.log.ods

import com.alibaba.fastjson.{JSON, JSONObject}
import com.example.log.util.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BaseDBMaxwellApp {

  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[3]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val topic = "gmall_db_m"
    val groupId = "base_db_maxwell_group"
    /**
     * 注：如果删除了topic，需要清空kafka根路径下的logs目录，groupId才能继续使用，否则报
     * Spark Streaming 'numRecords must not be negative'
     */

    val currentOffset = OffsetManagerUtil.getOffsetFromRedis(topic, groupId)

    var kafkaStream:DStream[ConsumerRecord[String,String]] = null
    if(currentOffset!=null && currentOffset.size>0){
      kafkaStream = MyKafkaUtil.getKafkaStream(topic, ssc,groupId,currentOffset)
    }else{
      kafkaStream = MyKafkaUtil.getKafkaStream(topic, ssc,groupId)
    }

    var offsetRanges:Array[OffsetRange] = null
    val kafkaDStream = kafkaStream.transform{rdd=>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    val objStream = kafkaDStream.map{record=>
      val nObj = new JSONObject()
      val obj = JSON.parseObject(record.value())
      val table = obj.getString("table")
      val opType = obj.getString("type")
      val data = obj.getJSONObject("data")
      nObj.put("table",table)
      nObj.put("type",opType)
      nObj.put("data",data)
      nObj
    }

    objStream.foreachRDD{rdd=>
      rdd.foreach{obj=>
        val table = obj.getString("table")
        val opType = obj.getString("type")
        val data = obj.getJSONObject("data")
        val topic = s"ods_maxwell_${table}"
        /**
         * maxwell bootstrap 功能
         * {"database":"gmall","table":"base_province","type":"bootstrap-complete","ts":1615194892,"data":{}}
           {"database":"gmall","table":"base_province","type":"bootstrap-start","ts":1615195231,"data":{}}
           {"database":"gmall","table":"base_province","type":"bootstrap-complete","ts":1615195231,"data":{}}
         */
        // 事实表只收集insert
        // 维度表除delete外都收集
        if(data!=null && data.size()>0 && !"delete".equals(opType)){
          // 一条sql插入多行记录，会被Maxwell解析为条记录
          MyKafkaUtil.send(topic,data.toString)
        }
      }
      OffsetManagerUtil.saveOffsetToRedis(topic, groupId, offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
