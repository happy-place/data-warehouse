package com.example.log.dim

import com.alibaba.fastjson.JSON
import com.example.log.model.ProvinceInfo
import com.example.log.util.{MyKafkaUtil, MyPropertiesUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.spark.toProductRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 从 kafka gmall_maxwell_province_info 接入到 phoenix
 */
object ProvinceInfoApp {

  def main(args:Array[String]):Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[4]")
    val ssc = new StreamingContext(conf,Seconds(5))

    val topic = "ods_maxwell_base_province"
    val groupId = "province_info_app_group"

    val phoenixTable = "gmall_province_info"

    val props = MyPropertiesUtil.load("config.properties")
    val quorum = props.getProperty("phoenix.quorum.url").substring(13)

    // step1：获取offset
    val currentOffsets = OffsetManagerUtil.getOffsetFromRedis(topic, groupId)

    // step2: 创建kafkaStream
    var kafkaStream:DStream[ConsumerRecord[String,String]] = null;
    if(currentOffsets!=null && currentOffsets.size>0){
      kafkaStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId, currentOffsets)
    }else{
      kafkaStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    // step3: 获取kafka offset
    var offsetRanges:Array[OffsetRange] = null
    val kafkaDStream = kafkaStream.transform{rdd =>
      offsetRanges =rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    // step4: 封装对象流
    val provinceInfoStream = kafkaDStream.map{record =>
      JSON.parseObject(record.value(), classOf[ProvinceInfo])
    }

    // step5: 写入 phoenix
    provinceInfoStream.foreachRDD{rdd =>
      rdd.saveToPhoenix(
        phoenixTable,
        Seq("ID","NAME","AREA_CODE","ISO_CODE"),
        new Configuration(),
        Some(quorum)
      )
      OffsetManagerUtil.saveOffsetToRedis(topic, groupId, offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
