package com.example.log.dim

import com.alibaba.fastjson.JSON
import com.example.log.model.BaseCategory3
import com.example.log.util.{MyKafkaUtil, MyPropertiesUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.spark.toProductRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BaseCatetory3App {

  def main(args:Array[String]):Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(conf,Seconds(5))

    val topic = "ods_maxwell_base_category3"
    val groupId = "base_catetory3_app"
    val phoenixTable = "gmall_base_category3"

    val props = MyPropertiesUtil.load("config.properties")
    val quorum = props.getProperty("phoenix.quorum.url").substring(13)

    val currentOffsets = OffsetManagerUtil.getOffsetFromRedis(topic, groupId)

    // 创建kafka流
    var kafkaDStream:DStream[ConsumerRecord[String,String]] = null
    if(currentOffsets!=null && currentOffsets.size>0){
      kafkaDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId, currentOffsets)
    }else{
      kafkaDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    // 获取kafka流offset
    var offsetRanges:Array[OffsetRange] = null
    val extractedKafkaStream = kafkaDStream.transform{rdd=>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    // 转换为对象流
    val categoryStream = extractedKafkaStream.map{record =>
      JSON.parseObject(record.value(),classOf[BaseCategory3])
    }

    // 存储phoenix，并在每个采集周期保存offset
    categoryStream.foreachRDD{rdd=>
      rdd.saveToPhoenix(phoenixTable,
          Seq("ID","NAME","CATEGORY2_ID"),
        new Configuration(),
        Some(quorum)
      )
      OffsetManagerUtil.saveOffsetToRedis(topic, groupId, offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()
  }


}
