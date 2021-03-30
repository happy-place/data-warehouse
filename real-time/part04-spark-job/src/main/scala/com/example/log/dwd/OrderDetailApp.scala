package com.example.log.dwd

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.example.log.model.{OrderDetail, SkuInfo}
import com.example.log.util.{MyKafkaUtil, MyPhoenixUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 订单明细表与sku维度退化表进行关联，将数据写回kafka，为order_info 与 order_detail 双流join做铺垫
 */
object OrderDetailApp {

  def main(args:Array[String]):Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))

    val sourceTopic = "ods_maxwell_order_detail"
    val groupId = "order_detail_app_group"

    val sinkTopic = "dwd_order_detail"

    // 获取offset
    val currentOffsets = OffsetManagerUtil.getOffsetFromRedis(sourceTopic,groupId)

    // 创建Kafka流
    var kafkaStream:DStream[ConsumerRecord[String,String]] = null
    if(currentOffsets!=null && currentOffsets.size>0){
      kafkaStream = MyKafkaUtil.getKafkaStream(sourceTopic, ssc, groupId, currentOffsets)
    }else{
      kafkaStream = MyKafkaUtil.getKafkaStream(sourceTopic, ssc, groupId)
    }

    // 提取 offset
    var offsetRanges:Array[OffsetRange] = null
    val extractedStream = kafkaStream.transform{rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach { or =>
        println(s"partition: ${or.partition} offset: ${or.fromOffset} => ${or.untilOffset}")
      }
      rdd
    }

    // 转换对象流
    val orderDetailStream = extractedStream.map{record =>
      JSON.parseObject(record.value(),classOf[OrderDetail])
    }

    // 关联 sku 维度退化表
    val orderDetailStreamWithSkuInfo = orderDetailStream.mapPartitions{iter =>
      val orderDetails = iter.toList
      if(orderDetails.size>0){
        val skuIds = s"'${orderDetails.map(_.sku_id).mkString("','")}'"
        val sql = s"select * from gmall_sku_info_expand where id in (${skuIds})"
        val skuMap = MyPhoenixUtil.queryList(sql).map{obj =>
          val skuInfo = SkuInfo(
            obj.getString("ID"),
            obj.getString("SPU_ID"),
            obj.getString("PRICE"),
            obj.getString("SKU_NAME"),
            obj.getString("TM_ID"),
            obj.getString("CATEGORY3_ID"),
            obj.getString("CREATE_TIME"),
            obj.getString("CATEGORY3_NAME"),
            obj.getString("SPU_NAME"),
            obj.getString("TM_NAME")
          )
          (skuInfo.id.toLong,skuInfo)
        }.toMap
        orderDetails.foreach{orderDetail =>
          val skuInfo = skuMap.getOrElse(orderDetail.sku_id,null)
          if(skuInfo!=null){
            orderDetail.spu_id = skuInfo.spu_id.toLong
            orderDetail.tm_id = skuInfo.tm_id.toLong
            orderDetail.category3_id = skuInfo.category3_id.toLong
            orderDetail.spu_name = skuInfo.spu_name
            orderDetail.tm_name = skuInfo.tm_name
            orderDetail.category3_name = skuInfo.category3_name
          }
          orderDetail
        }
      }
      orderDetails.iterator
    }

    // 结果写回kafka，并保存offset
    orderDetailStreamWithSkuInfo.foreachRDD{rdd =>
      rdd.foreach{orderDetail =>
        MyKafkaUtil.send(sinkTopic,JSON.toJSONString(orderDetail,new SerializeConfig(true)))
      }
      OffsetManagerUtil.saveOffsetToRedis(sourceTopic,groupId, offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()
  }



}
