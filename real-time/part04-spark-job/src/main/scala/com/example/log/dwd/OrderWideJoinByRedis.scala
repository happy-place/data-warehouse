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
import redis.clients.jedis.Jedis

import java.util
import java.util.Properties
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object OrderWideJoinByRedis {

  def main(args:Array[String]):Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))

    val cacheSeconds = 600
    val groupId = "order_wide_join_by_redis_group"
    val orderInfoTopic = "dwd_order_info"

    // 获取offset
    val orderInfoCurrentOffsets = OffsetManagerUtil.getOffsetFromRedis(orderInfoTopic, groupId)

    // 创建kafka流，提取Offset,并转换为kv形式对象流
    var orderInfoKafkaStream:DStream[ConsumerRecord[String,String]] = null
    if(orderInfoCurrentOffsets!=null && orderInfoCurrentOffsets.size>0){
      orderInfoKafkaStream = MyKafkaUtil.getKafkaStream(orderInfoTopic,ssc,groupId,orderInfoCurrentOffsets)
    }else{
      orderInfoKafkaStream = MyKafkaUtil.getKafkaStream(orderInfoTopic,ssc,groupId)
    }

    var orderInfoOffsetRanges:Array[OffsetRange] = null
    val orderInfoStream = orderInfoKafkaStream.transform{rdd=>
      orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.map{record =>
      val orderInfo = JSON.parseObject(record.value(),classOf[OrderInfo])
      (orderInfo.id,orderInfo)
    }

    val orderDetailTopic = "dwd_order_detail"
    val orderDetailCurrentOffsets = OffsetManagerUtil.getOffsetFromRedis(orderDetailTopic, groupId)

    var orderDetailKafkaStream:DStream[ConsumerRecord[String,String]] = null
    if(orderDetailCurrentOffsets!=null && orderDetailCurrentOffsets.size>0){
      orderDetailKafkaStream = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, groupId, orderDetailCurrentOffsets)
    }else{
      orderDetailKafkaStream = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, groupId)
    }

    var orderDetailOffsetRanges:Array[OffsetRange] = null
    val orderDetailStream = orderDetailKafkaStream.transform{rdd =>
      orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.map{record =>
      val orderDetail = JSON.parseObject(record.value(),classOf[OrderDetail])
      (orderDetail.order_id,orderDetail)
    }

    // 双流外连接，借助redis缓存封装orderWide
   val orderWideStream = orderInfoStream.fullOuterJoin(orderDetailStream).mapPartitions{iter =>
      val orderWides = new ListBuffer[OrderWide]
      val client = MyRedisUtil.getJedisClient
      iter.foreach{
        case (orderId,(orderInfoOption,orderDetailOption)) =>
          val orderInfoKey = s"order_info_${orderId}"
          val orderDetailKey = s"order_detail_of_${orderId}"

          if(orderInfoOption!=None){
            val orderInfo = orderInfoOption.get
            client.setex(orderInfoKey,cacheSeconds,JSON.toJSONString(orderInfo,new SerializeConfig(true)))

            if(orderDetailOption!=None){
              val orderDetail = orderDetailOption.get
              val orderWide = new OrderWide(orderInfo,orderDetail)
              orderWides.append(orderWide) // 流-OrderInfo + 流-OrderDetail
            }
            client.smembers(orderDetailKey).forEach{str=>
              val orderDetail = JSON.parseObject(str,classOf[OrderDetail])
              orderWides.append(new OrderWide(orderInfo,orderDetail))
              client.srem(orderDetailKey,str) // 流-OrderInfo + redis-OrderDetail
            }
          }else{
            val orderDetail = orderDetailOption.get
            val orderInfoStr = client.get(orderInfoKey)
            if(orderInfoStr!=null && orderInfoStr.size>0){
              val orderInfo = JSON.parseObject(orderInfoStr, classOf[OrderInfo])
              orderWides.append(new OrderWide(orderInfo,orderDetail))  // redis-OrderInfo + 流-OrderDetail
            }else{
              client.sadd(orderDetailKey,JSON.toJSONString(orderDetail,new SerializeConfig(true)))
              client.expire(orderDetailKey,cacheSeconds)
            }
          }
      }
      client.close()
      orderWides.iterator
    }

    orderWideStream.foreachRDD { rdd =>
      rdd.foreach{obj =>
        println(JSON.toJSONString(obj,new SerializeConfig(true)))
      }
    }

/*
    val orderWideStream: DStream[OrderWide] = orderInfoStream.fullOuterJoin(orderDetailStream).flatMap {
      case (orderId, (orderInfoOpt, orderDetailOpt)) =>
        val orderWideList: ListBuffer[OrderWide] = ListBuffer[OrderWide]()
        val jedis: Jedis = MyRedisUtil.getJedisClient
        if (orderInfoOpt != None) {
          val orderInfo: OrderInfo = orderInfoOpt.get
          if (orderDetailOpt != None) {
            val orderDetail: OrderDetail = orderDetailOpt.get
            val orderWide = new OrderWide(orderInfo, orderDetail)
            orderWideList.append(orderWide) //1 join成功产生一条宽表数据
          }
          //2.1 主表写缓存
          // redis  type?  string     key ?  order_info:0101    value? orderInfoJson  expire?:600
          //   实现 能够让从表方便的通过orderId 查到订单主表信息
          val orderInfoKey = "order_info:" + orderInfo.id
          val orderInfoJson = JSON.toJSONString(orderInfo, new SerializeConfig(true))
          jedis.setex(orderInfoKey, 600, orderInfoJson)
          //2.2 主表读缓存
          // 从表缓存如何设计：    实现主表能够方便的通过orderId
          //redis  type?   set   key?  order_detail:0101  value? 多个orderDetailJson     expire? 600
          val orderDetailKey = "order_detail:" + orderInfo.id
          val orderDetailJsonSet: util.Set[String] = jedis.smembers(orderDetailKey)
          import collection.JavaConverters._
          for (orderDetailJson <- orderDetailJsonSet.asScala) {
            val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
            val orderWide = new OrderWide(orderInfo, orderDetail)
            orderWideList.append(orderWide)
          }

        } else {
          val orderDetail: OrderDetail = orderDetailOpt.get
          //3.1 从表查询主表
          val orderInfoKey = "order_info:" + orderDetail.order_id
          val orderInfoJson: String = jedis.get(orderInfoKey)
          if (orderInfoJson != null && orderInfoJson.length > 0) {
            val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
            val orderWide = new OrderWide(orderInfo, orderDetail)
            orderWideList.append(orderWide)
          }
          //3.2 从表写缓存
          // 从表缓存如何设计：    实现主表能够方便的通过orderId
          //redis  type?   set   key?  order_detail:[order_id]  value? 多个orderDetailJson     expire? 600
          val orderDetailKey = "order_detail:" + orderDetail.order_id
          val orderDetailJson: String = JSON.toJSONString(orderDetail, new SerializeConfig(true))
          jedis.sadd(orderDetailKey, orderDetailJson)
          jedis.expire(orderDetailKey, 600)
        }
        jedis.close()
        orderWideList
    }*/
//
//    orderWideStream.print(1)
//
//    // 最终费用分摊计算
//    val splitStream = orderWideStream.mapPartitions{iter =>
//      val client = MyRedisUtil.getJedisClient
//      val orderWides = iter.map{orderWide =>
//        val original_detail_amount = orderWide.sku_price * orderWide.sku_num
//        var original_left_amount = orderWide.original_total_amount
//        var final_left_amount = orderWide.final_total_amount
//
//        val key = s"amount_split_${orderWide.order_id}"
//        var value = client.get(key)
//        if(value!=null && value.size>0){
//          val arr = value.split(":")
//          original_left_amount = arr(0).toDouble
//          final_left_amount = arr(1).toDouble
//          if (original_detail_amount == orderWide.original_total_amount - original_left_amount) {
//            orderWide.final_detail_amount = Math.round((orderWide.final_total_amount - final_left_amount)*100D) / 100D
//          } else {
//            orderWide.final_detail_amount = Math.round(original_detail_amount * (orderWide.final_total_amount / orderWide.original_total_amount)* 100D) / 100D
//          }
//        }else{
//          orderWide.final_detail_amount = Math.round(original_detail_amount * (orderWide.final_total_amount / orderWide.original_total_amount)* 100D) / 100D
//        }
//
//        // 写出到redis必须取整
//        original_left_amount = Math.round((original_left_amount - original_detail_amount)* 100D) / 100D
//        final_left_amount = Math.round((final_left_amount - orderWide.final_detail_amount)* 100D) / 100D
//        value = s"${original_left_amount}:${final_left_amount}"
//        println(value)
//        client.setex(key, cacheSeconds, value)
//        orderWide
//      }.toList
//      client.close()
//      orderWides.iterator
//    }
//
//    splitStream.map(o=>(o.order_id,o)).groupByKey().filter(_._2.size>1).flatMap(_._2).foreachRDD { rdd =>
//      rdd.foreach{obj =>
//        println(JSON.toJSONString(obj,new SerializeConfig(true)))
//      }
//    }
//
//    splitStream.foreachRDD { rdd =>
//      OffsetManagerUtil.saveOffsetToRedis(orderInfoTopic,groupId,orderInfoOffsetRanges)
//      OffsetManagerUtil.saveOffsetToRedis(orderDetailTopic,groupId,orderDetailOffsetRanges)
//    }

      //    结果存储phoenix，并保存offset
//    val sparkSession = SparkSession.builder().appName("order_wide_join_by_redis").getOrCreate()
//    import sparkSession.implicits._
//
//    splitStream.foreachRDD{rdd =>
//      rdd.toDF().write.mode(SaveMode.Append)
//        .option("batchsize","100")
//        .option("isolationLevel","NONE")
//        .option("numPartitions","3")
//        .option("driver","ru.yandex.clickhouse.ClickHouseDriver")
//        .jdbc("jdbc:clickhouse://hadoop03:8123/default","t_order_wide",new Properties())
//      OffsetManagerUtil.saveOffsetToRedis(orderInfoTopic,groupId,orderInfoOffsetRanges)
//      OffsetManagerUtil.saveOffsetToRedis(orderDetailTopic,groupId,orderDetailOffsetRanges)
//    }

    ssc.start()
    ssc.awaitTermination()
  }


}
