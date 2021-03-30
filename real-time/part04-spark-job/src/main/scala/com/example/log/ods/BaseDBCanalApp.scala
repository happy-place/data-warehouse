package com.example.log.ods

import com.alibaba.fastjson.{JSON, JSONObject}
import com.example.log.util.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 消费kafka中canal采集的增量同步数据，以表名为topic分流
 */
object BaseDBCanalApp {

   def main(args:Array[String]):Unit={
     val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[3]")
     val ssc = new StreamingContext(conf, Seconds(5))

     val topic = "gmall_db_c"
     val groupId = "base_db_canal_group"

     // step1: 获取kafka offset
     val currentOffset = OffsetManagerUtil.getOffsetFromRedis(topic, groupId)

     // step2: 创建kafka 流
     var kafkaStream:DStream[ConsumerRecord[String,String]] = null
     if(currentOffset!=null && currentOffset.size>0){
       kafkaStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId,currentOffset)
     }else{
       kafkaStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
     }

     // step3: 提取当前kafka流的offset
     var offsetRanges:Array[OffsetRange] = null
     val kafkaDStream = kafkaStream.transform{rdd=>
       offsetRanges= rdd.asInstanceOf[HasOffsetRanges].offsetRanges
       rdd
     }

     // step4: 简化binlog日志
     val objStream = kafkaDStream.map{record =>
       val target = new JSONObject()
       val obj = JSON.parseObject(record.value())
       target.put("table",obj.getString("table"))
       target.put("data",obj.getJSONArray("data"))
       target.put("type",obj.getString("type"))
       target
     }

     // step5: 分流到以数据库表命名的topic，并手动保存offset
     objStream.foreachRDD{rdd=>
       rdd.foreach{obj =>
         val table = obj.getString("table")
         val topic = s"ods_canal_${table}"
         val opType = obj.getString("type")
         val dataArr = obj.getJSONArray("data")
         // 只保存INSERT
         if("INSERT".equals(opType)){
           // canal会将一条语句涉及到的多行收集为集合，为方便后面使用，因此在此处展开
           dataArr.forEach{msg =>
             MyKafkaUtil.send(topic,msg.toString)
           }
         }
       }
       OffsetManagerUtil.saveOffsetToRedis(topic,groupId,offsetRanges)
     }

     ssc.start()
     ssc.awaitTermination()
   }

}
