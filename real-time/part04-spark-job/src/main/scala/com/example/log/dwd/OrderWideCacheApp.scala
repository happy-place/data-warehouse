package com.example.log.dwd


import java.{lang, util}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.example.log.model.{OrderDetail, OrderInfo, OrderWide}
import com.example.log.util.{MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * 双流Join ，使用缓存处理数据丢失问题
 * 与开窗+去重不同的是，使用缓存处理，两个流做满外连接 ，因为网络延迟等关系，不能保证每个窗口中的数据key都能匹配上，
 * 这样势必会出现三种情况：（some，some），（None，some），（Some,None）,根据这三种情况，下面做一下详细解析：
 * （some，some）
 *     1号流和2号流中key能正常进行逻辑运算，但是考虑到2号流后续可能会有剩下的数据到来，所以需要将1号流中的key保存到redis，以等待接下来的数据
 *
 * （None，Some）
 *     找不到1号流中对应key的数据，需要去redis中查找1号流的缓存，如果找不到，则缓存起来，等待1号流
 *
 * （Some，None）
 *     找不到2号流中的数据，需要将key保存到redis，以等待接下来的数据，并且去reids中找2号流的缓存，如果有，则join，然后删除2号流的缓存
 */
object OrderWideCacheApp {


  def main(args: Array[String]): Unit = {
    //双流  订单主表  订单从表    偏移量 双份
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dws_order_wide_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val orderInfoGroupId = "dws_order_info_group"
    val orderInfoTopic = "dwd_order_info"
    val orderDetailGroupId = "dws_order_detail_group"
    val orderDetailTopic = "dwd_order_detail"

    //1   从redis中读取偏移量   （启动执行一次）
    val orderInfoOffsetMapForKafka: Map[TopicPartition, Long] = OffsetManagerUtil.getOffsetFromRedis(orderInfoTopic, orderInfoGroupId)
    val orderDetailOffsetMapForKafka: Map[TopicPartition, Long] = OffsetManagerUtil.getOffsetFromRedis(orderDetailTopic, orderDetailGroupId)

    //2   把偏移量传递给kafka ，加载数据流（启动执行一次）
    var orderInfoRecordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderInfoOffsetMapForKafka != null && orderInfoOffsetMapForKafka.size > 0) { //根据是否能取到偏移量来决定如何加载kafka 流
      orderInfoRecordInputDstream = MyKafkaUtil.getKafkaStream(orderInfoTopic, ssc,orderInfoGroupId, orderInfoOffsetMapForKafka)
    } else {
      orderInfoRecordInputDstream = MyKafkaUtil.getKafkaStream(orderInfoTopic, ssc, orderInfoGroupId)
    }


    var orderDetailRecordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderDetailOffsetMapForKafka != null && orderDetailOffsetMapForKafka.size > 0) { //根据是否能取到偏移量来决定如何加载kafka 流
      orderDetailRecordInputDstream = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailGroupId,orderDetailOffsetMapForKafka)
    } else {
      orderDetailRecordInputDstream = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailGroupId)
    }


    //3   从流中获得本批次的 偏移量结束点（每批次执行一次）
    var orderInfoOffsetRanges: Array[OffsetRange] = null //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
    val orderInfoInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderInfoRecordInputDstream.transform { rdd => //周期性在driver中执行
      orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    var orderDetailOffsetRanges: Array[OffsetRange] = null //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
    val orderDetailInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderDetailRecordInputDstream.transform { rdd => //周期性在driver中执行
      orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }


    // 1 提取数据 2 分topic
    val orderInfoDstream: DStream[OrderInfo] = orderInfoInputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      //订单处理  脱敏  换成特殊字符  直接去掉   转换成更方便操作的专用样例类
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
      orderInfo
    }

    val orderDetailDstream: DStream[OrderDetail] = orderDetailInputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      //订单处理  脱敏  换成特殊字符  直接去掉   转换成更方便操作的专用样例类
      val orderDetail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
      orderDetail
    }

    //
    //    val orderInfoWithKeyDstream: DStream[(Long, OrderInfo)] = orderInfoDstream.map(orderInfo=>(orderInfo.id,orderInfo))
    //    val orderDetailWithKeyDstream: DStream[(Long, OrderDetail)] = orderDetailDstream.map(orderDetail=>(orderDetail.order_id,orderDetail))
    //
    //    val joinedDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDstream.join(orderDetailWithKeyDstream,4)


    // 方案二：  1 join   2.1 主表写缓存   2.2 主表查缓存   3.1 从表写缓存  3.2 从表读缓存

    //想join先变k-v
    val orderInfoWithKeyDstream: DStream[(Long, OrderInfo)] = orderInfoDstream.map(orderInfo => (orderInfo.id, orderInfo))
    val orderDetailWithKeyDstream: DStream[(Long, OrderDetail)] = orderDetailDstream.map(orderDetail => (orderDetail.order_id, orderDetail))

    val fullJoinedDstream: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoWithKeyDstream.fullOuterJoin(orderDetailWithKeyDstream, 4)


    val orderWideDstream: DStream[OrderWide] = fullJoinedDstream.flatMap {
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
    }
    orderWideDstream.print(1000)

    orderWideDstream.foreachRDD { rdd =>
      OffsetManagerUtil.saveOffsetToRedis(orderInfoTopic, orderInfoGroupId, orderInfoOffsetRanges)
      OffsetManagerUtil.saveOffsetToRedis(orderDetailTopic, orderDetailGroupId, orderDetailOffsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
  }


}