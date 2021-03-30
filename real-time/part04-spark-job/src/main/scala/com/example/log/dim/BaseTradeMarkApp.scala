package com.example.log.dim

import com.alibaba.fastjson.JSON
import com.example.log.model.BaseTrademark
import com.example.log.util.{MyKafkaUtil, MyPropertiesUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.spark.toProductRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BaseTradeMarkApp {

  def main(args:Array[String ]):Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))

    val topic = "ods_maxwell_base_trademark"
    val groupId = "base_trade_mark_app"
    val phoenixTable = "gmall_base_trademark"

    // phoenix.quorum.url
    val props = MyPropertiesUtil.load("config.properties")
    val quorum =props.getProperty("phoenix.quorum.url").substring(13)

    // 获取offset
    val currentOffsets = OffsetManagerUtil.getOffsetFromRedis(topic, groupId)

    // 创建kafka流
    var kafkaDStream:DStream[ConsumerRecord[String,String]] = null
    if(currentOffsets!=null&&currentOffsets.size>0){
      kafkaDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId,currentOffsets)
    }else{
      kafkaDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    // 提取offset
    var offsetRanges:Array[OffsetRange] = null
    val extractedKafkaStream = kafkaDStream.transform{rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    // 创建对象流
    val tradeMarkStream = extractedKafkaStream.map{record =>
      JSON.parseObject(record.value(), classOf[BaseTrademark])
    }

    // 写入phoenix，并在每个扫描周期保存offset
    tradeMarkStream.foreachRDD{rdd=>
      rdd.saveToPhoenix(phoenixTable,
        Seq("ID","TM_NAME"), // phoenix 中字段你是ID
        new Configuration(),
        Some(quorum)
      )
      OffsetManagerUtil.saveOffsetToRedis(topic,groupId,offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
