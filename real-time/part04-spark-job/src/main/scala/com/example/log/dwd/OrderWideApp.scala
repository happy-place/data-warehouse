package com.example.log.dwd

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.example.log.model.{OrderDetail, OrderInfo, OrderWide}
import com.example.log.util.{MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Properties

object OrderWideApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(conf, Seconds(5))

    val sinkKafkaTopic = "dwd_order_wide"

    val expireSeconds = 600
    val groupId = "order_wide_app_group"

    // 创建双流
    val orderInfoTopic = "dwd_order_info"
    val orderInfoCurrentOffsets = OffsetManagerUtil.getOffsetFromRedis(orderInfoTopic, groupId)

    var orderInfoKafkaStream:DStream[ConsumerRecord[String,String]] = null
    if(orderInfoCurrentOffsets!=null&&orderInfoCurrentOffsets.size>0){
      orderInfoKafkaStream = MyKafkaUtil.getKafkaStream(orderInfoTopic,ssc,groupId,orderInfoCurrentOffsets)
    }else{
      orderInfoKafkaStream = MyKafkaUtil.getKafkaStream(orderInfoTopic,ssc,groupId)
    }

    var orderInfoOffsetRanges:Array[OffsetRange] = null
    val extractedOrderInfoKafkaStream = orderInfoKafkaStream.transform{rdd =>
      orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    val orderDetailTopic = "dwd_order_detail"
    val orderDetailCurrentOffsets = OffsetManagerUtil.getOffsetFromRedis(orderDetailTopic, groupId)
    var orderDetailKafkaStream:DStream[ConsumerRecord[String,String]] = null
    if(orderDetailCurrentOffsets!=null && orderDetailCurrentOffsets.size>0){
      orderDetailKafkaStream = MyKafkaUtil.getKafkaStream(orderDetailTopic,ssc,groupId,orderDetailCurrentOffsets)
    }else{
      orderDetailKafkaStream = MyKafkaUtil.getKafkaStream(orderDetailTopic,ssc,groupId)
    }

    var orderDetailOffsetRanges:Array[OffsetRange] = null
    val extractedOrderDetailKafkaStream = orderDetailKafkaStream.transform{rdd =>
      orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    // 双流封装对象,并转换为kv形式
    val orderInfoStream = extractedOrderInfoKafkaStream.map{record =>
      val order = JSON.parseObject(record.value(), classOf[OrderInfo])
      (order.id,order)
    }

    val orderDetailStream = extractedOrderDetailKafkaStream.map{record =>
      val orderDetail = JSON.parseObject(record.value(),classOf[OrderDetail])
      (orderDetail.order_id,orderDetail)
    }

    // 转化为滚动窗口流
    val orderInfoStreamWithWindow = orderInfoStream.window(Seconds(50),Seconds(5))
    val orderDetailStreamWithWindow = orderDetailStream.window(Seconds(50),Seconds(5))

    // 双流join，得到orderWide流
    val joinedStream = orderInfoStreamWithWindow.join(orderDetailStreamWithWindow)
      .groupByKey()
      .map(_._2).flatMap{iter =>
        iter.map{ tup =>
          val (orderInfo,orderDetail) = tup
          val orderWide = OrderWide()
          orderWide.mergeOrderInfo(orderInfo)
          orderWide.mergeOrderDetail(orderDetail)
          orderWide
        }
      }

        // 基于redis去重
        val distinctedStream = joinedStream.mapPartitions{iter =>
          val orderWides = iter.toList
          val client = MyRedisUtil.getJedisClient
          val results = orderWides.map{orderWide =>
            val key = s"orderwide_distinct_${orderWide.order_id}"
            val notExisted = client.sadd(key,orderWide.sku_id.toString) == 1L
            if(notExisted){
              client.expire(key,expireSeconds)
            }
            (notExisted,orderWide)
          }
          client.close()
          results.iterator
        }.filter(_._1)
          .map(_._2)


    // 基于orderWide流计算分摊金额，final_detail_amount
    val splitedStream = distinctedStream.mapPartitions{iter =>
      val client = MyRedisUtil.getJedisClient
      val orderWides = iter.map{orderWide =>
        val original_detail_amount = orderWide.sku_price * orderWide.sku_num
        var original_left_amount = orderWide.original_total_amount
        var final_left_amount = orderWide.final_total_amount

        val key = s"amount_split_${orderWide.order_id}"
        var value = client.get(key)
        if(value!=null && value.size>0){
          val arr = value.split(":")
          original_left_amount = arr(0).toDouble
          final_left_amount = arr(1).toDouble
         if (original_detail_amount == orderWide.original_total_amount - original_left_amount) {
            orderWide.final_detail_amount = Math.round((orderWide.final_total_amount - final_left_amount)*100D) / 100D
          } else {
            orderWide.final_detail_amount = Math.round(original_detail_amount * (orderWide.final_total_amount / orderWide.original_total_amount)* 100D) / 100D
          }
        }else{
          orderWide.final_detail_amount = Math.round(original_detail_amount * (orderWide.final_total_amount / orderWide.original_total_amount)* 100D) / 100D
        }

        // 写出到redis必须取整
        original_left_amount = Math.round((original_left_amount - original_detail_amount)* 100D) / 100D
        final_left_amount = Math.round((final_left_amount - orderWide.final_detail_amount)* 100D) / 100D
        value = s"${original_left_amount}:${final_left_amount}"
        client.setex(key, expireSeconds, value)
        orderWide
      }.toList
      client.close()
      orderWides.iterator
    }

//    splitedStream.map(o => (o.order_id,o)).groupByKey().filter(_._2.size>1).flatMap(_._2)
//      .foreachRDD{rdd=>
//        rdd.foreach{orderWide =>
//          println(JSON.toJSONString(orderWide,new SerializeConfig(true)))
//        }
//      }

    // 输出到clickhouse,并保存offset
    val sparkSession = SparkSession.builder().appName("order_wide_app_df").getOrCreate()
    import sparkSession.implicits._
    splitedStream.foreachRDD{rdd =>
      rdd.cache()

      rdd.toDF()
        .write
        .mode(SaveMode.Append)
        .option("batchsize","100")
        .option("isolationLevel","NONE")
        .option("numPartitions","3")
        .option("driver","ru.yandex.clickhouse.ClickHouseDriver")
        .jdbc("jdbc:clickhouse://hadoop03:8123/default","t_order_wide",new Properties())

      //  写出到kafka，提供ads层汇总统计
      rdd.foreachPartition{iter =>
        iter.foreach{orderWide =>
          MyKafkaUtil.send(sinkKafkaTopic,JSON.toJSONString(orderWide,new SerializeConfig(true)))
        }
      }

      OffsetManagerUtil.saveOffsetToRedis(orderInfoTopic,groupId,orderInfoOffsetRanges)
      OffsetManagerUtil.saveOffsetToRedis(orderDetailTopic,groupId,orderDetailOffsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
