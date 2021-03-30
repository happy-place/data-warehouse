package com.example.log.dwd

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.example.log.model.{OrderInfo, ProvinceInfo, UserInfo, UserStatus}
import com.example.log.util.{MyESUtil, MyKafkaUtil, MyPhoenixUtil, MyPropertiesUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.spark.toProductRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderInfoApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(conf, Seconds(5))

    val topic = "ods_maxwell_order_info"
    val groupId = "order_info_app_group"
    val props = MyPropertiesUtil.load("config.properties")
    val quorum = props.getProperty("phoenix.quorum.url").substring(13)

    // 获取 kafka offset
    val currentOffsets = OffsetManagerUtil.getOffsetFromRedis(topic, groupId)

    // 创建kafka stream
    var kafkaStream: DStream[ConsumerRecord[String, String]] = null
    if (currentOffsets != null && currentOffsets.size > 0) {
      kafkaStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId, currentOffsets)
    } else {
      kafkaStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    // 提取kafka流的 offset
    var offsetRanges: Array[OffsetRange] = null
    val kafkaDStream = kafkaStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach { or =>
        println(s"partition: ${or.partition} offset: ${or.fromOffset} => ${or.untilOffset}")
      }
      rdd
    }

    // 创建对象流
    val orderStream = kafkaDStream.map { record =>
      val order = JSON.parseObject(record.value(), classOf[OrderInfo])
      val dt = order.create_time
      order.create_date = dt.substring(0, 10)
      order.create_hour = dt.substring(11, 13)
      order
    }

    // 关联省份维度
    val orderStreamWithProv = orderStream.transform { rdd =>
      val sql = "select * from gmall_province_info"
      val provMap = MyPhoenixUtil.queryList(sql).map { obj =>
        val province = ProvinceInfo(obj.getString("ID"),
          obj.getString("NAME"),
          obj.getString("AREA_CODE"),
          obj.getString("ISO_CODE"))
        (province.id.toLong, province)
      }.toMap

      val provMapBC = ssc.sparkContext.broadcast(provMap)

      rdd.mapPartitions { iter =>
        iter.map { order =>
          val provMap = provMapBC.value
          val prov = provMap.getOrElse(order.province_id, null)
          if (prov != null) {
            order.province_iso_code = prov.iso_code
            order.province_name = prov.name
            order.province_area_code = prov.area_code
          }
          order
        }
      }
    }

    // 关联用户维度
    val orderStreamWithProvUser = orderStreamWithProv.mapPartitions { iter =>
      val orders = iter.toList
      val user_ids = s"'${orders.map(_.user_id).mkString("','")}'"
      val sql = s"select * from gmall_user_info where id in (${user_ids})"
      val userMap = MyPhoenixUtil.queryList(sql).map { obj =>
        val userInfo = UserInfo(obj.getString("ID"),
          obj.getString("USER_LEVEL"),
          obj.getString("BIRTHDAY"),
          obj.getString("GENDER"),
          obj.getString("AGE_GROUP"),
          obj.getString("GENDER_NAME")
        )
        (userInfo.id.toLong, userInfo)
      }.toMap

      orders.foreach { order =>
        val userInfo = userMap.getOrElse(order.user_id, null)
        if (userInfo != null) {
          order.user_gender = userInfo.gender_name
          order.user_age_group = userInfo.age_group
        }
      }
      orders.iterator
    }

    // 修订首单状态
    val orderStreamWithProvUserStatus = orderStreamWithProvUser.mapPartitions { iter =>
      val orders = iter.toList
      val user_ids = s"'${orders.map(_.user_id).mkString("','")}'"
      val sql = s"select * from gmall_user_status where user_id in (${user_ids})"

      val statusMap = MyPhoenixUtil.queryList(sql).map { obj =>
        val userStatus = UserStatus(obj.getString("USER_ID"), obj.getString("IF_CONSUMED"))
        (userStatus.userId.toLong, userStatus)
      }.toMap

      orders.foreach { order =>
        val userStatus = statusMap.getOrElse(order.user_id, null)
        if (userStatus != null) {
          order.if_first_order = "0"
        } else {
          order.if_first_order = "1"
        }
      }
      orders.iterator
    }

    // 修订同批首单误标情况
    val orderInfoStream = orderStreamWithProvUserStatus.map(order => (order.user_id, order))
      .groupByKey()
      .flatMap { tupl =>
        var orders = tupl._2.toList
        if (orders.size > 0) {
          orders = orders.sortWith((o1, o2) => (o1.create_time < o2.create_time))
          val order = orders(0)
          if (order.if_first_order == "1") {
            for (i <- 1 until orders.size) {
              orders(i).if_first_order = "0"
            }
          }
        }
        orders
      }

    // 保持首单信息
    orderInfoStream.foreachRDD { rdd =>
      rdd.filter(_.if_first_order == "1").map(order=>UserStatus(order.user_id.toString,order.if_first_order))
        .saveToPhoenix("gmall_user_status", Seq("USER_ID", "IF_CONSUMED"), new Configuration(), Some(quorum))

      // 保存订单信息到ES
      rdd.foreachPartition{iter =>
        val orders = iter.toList
        val bulkMap = orders.map{order=>
          val dt = order.create_date
          val key = s"${order.id}:gmall_${dt.substring(0, 4)}_order_info_${dt}"
          (key,order)
        }.toMap
        MyESUtil.bulkInsert(bulkMap)

        val topic="dwd_order_info"
        orders.foreach{order=>
          val value = JSON.toJSONString(order, new SerializeConfig(true)) // 自动忽略null
          MyKafkaUtil.send(topic,value)
        }
      }

      // 保存kafka offset
      OffsetManagerUtil.saveOffsetToRedis(topic, groupId, offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()
  }


}
