package com.example.log.dwd

import com.alibaba.fastjson.JSON
import com.example.log.model.{OrderInfo, ProvinceInfo, UserInfo, UserStatus}
import com.example.log.util.{MyESUtil, MyKafkaUtil, MyPhoenixUtil, MyPropertiesUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.spark.toProductRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 是否是首单
 *
 */
object OrderInfoAppJoinWithBroadcast {

  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))

    val topic = "ods_maxwell_order_info"
    val groupId = "order_info_app_group"

    val phoenixTable = "gmall_user_status"

    val props = MyPropertiesUtil.load("config.properties")
    val quorum = props.getProperty("phoenix.quorum.url").substring(13)

    // step1: 从redis读取offset
    val currentOffsets = OffsetManagerUtil.getOffsetFromRedis(topic, groupId)

    // step2: 从订单topic创建kafka流
    var kafkaStream:DStream[ConsumerRecord[String,String]] = null
    if(currentOffsets!=null && currentOffsets.size>0){
      kafkaStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId, currentOffsets)
    }else{
      kafkaStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    // step3: 提取当前kafa流的offsetRange
    var offsetRanges:Array[OffsetRange] = null
    val kafkaDStream = kafkaStream.transform{rdd=>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    // step4: 基于kafka流，创建DStream[OrderInfo]
    val orderInfoStream = kafkaDStream.map{record =>
        val orderInfo = JSON.parseObject(record.value(),classOf[OrderInfo])
        orderInfo.create_date = orderInfo.create_time.substring(0,10)
        orderInfo.create_hour = orderInfo.create_time.substring(11,13)
        orderInfo
    }

    // operate_time 有存在缺失情况
//    orderInfoStream.filter(_.operate_time!=null).print()
//    orderInfoStream.filter(_.operate_time==null).print()

    // step6: 关联 province信息
  /*  // 方案1：以partition为批次查询phoenix
    val orderWithProvinceStream = orderInfoStream.mapPartitions{iter =>
      val orders = iter.toList
      val province_ids = s"'${orders.map(_.province_id).mkString("','")}'"
      val sql = s"select ID,NAME,AREA_CODE,ISO_CODE from gmall_province_info where id in (${province_ids})"
      val provinceMap = MyPhoenixUtil.queryList(sql).map{row =>
        val id = row.getString("ID")
        val name = row.getString("NAME")
        val area_code = row.getString("AREA_CODE")
        val iso_code = row.getString("ISO_CODE")
        val provinceInfo = ProvinceInfo(id,name,area_code,iso_code)
        (provinceInfo.id.toLong,provinceInfo)
      }.toMap

      orders.foreach{order =>
        val province = provinceMap.getOrElse(order.province_id,null)
        // 确保关联不上也不报错
        if(province!=null){
          order.province_name = province.name
          order.province_iso_code = province.iso_code
          order.province_area_code = province.area_code
        }
      }
      orders.iterator
    }*/

    // 方案2：将省份信息定义为广播大变量（仅针对于大小在200~300M之间数据集）

    val sql = s"select ID,NAME,AREA_CODE,ISO_CODE from gmall_province_info"
    val provinceMap = MyPhoenixUtil.queryList(sql).map{row =>
      val id = row.getString("ID")
      val name = row.getString("NAME")
      val area_code = row.getString("AREA_CODE")
      val iso_code = row.getString("ISO_CODE")
      val provinceInfo = ProvinceInfo(id,name,area_code,iso_code)
      (provinceInfo.id.toLong,provinceInfo)
    }.toMap

    val orderWithProvinceStream = orderInfoStream.transform{rdd =>
      val sql = s"select ID,NAME,AREA_CODE,ISO_CODE from gmall_province_info"
      val provinceMap = MyPhoenixUtil.queryList(sql).map{row =>
        val id = row.getString("ID")
        val name = row.getString("NAME")
        val area_code = row.getString("AREA_CODE")
        val iso_code = row.getString("ISO_CODE")
        val provinceInfo = ProvinceInfo(id,name,area_code,iso_code)
        (provinceInfo.id.toLong,provinceInfo)
      }.toMap

      /**
       * 注册广播变量
       * 每个采集周期，都会从Driver端发起一次phoenix查询，然后将数据集交给Executor，之前每个分区发起一次查询，被简化为每个Executor发起一次查询
       * 分区越多，效果效率提升月明显
       * 当确定 被广播对象不会变更时，可以将广播变量注册，写在顶级代码中，这样只在启动时加载一次
       */
      val provinceMapBC = ssc.sparkContext.broadcast(provinceMap)

      val orderWithProvInfoRDD = rdd.mapPartitions{iter=>
        // 获取广播变量
        val provinceMap = provinceMapBC.value
        iter.map{order =>
          val provinceInfo = provinceMap.getOrElse(order.province_id,null)
          if(provinceInfo!=null){
            order.province_area_code = provinceInfo.area_code
            order.province_name = provinceInfo.name
            order.province_iso_code = provinceInfo.iso_code
          }
          order
        }
      }
      orderWithProvInfoRDD
    }

    // step7: 关联用户信息，通常用户量比较大，不适合广播变量，只能按partition分批查
    val orderWithUserInfoStream = orderWithProvinceStream.mapPartitions{iter=>
      val orders = iter.toList
      val user_ids = s"'${orders.map(_.user_id).mkString("','")}'"

      val sql = s"select ID,USER_LEVEL,BIRTHDAY,GENDER,AGE_GROUP,GENDER_NAME from gmall_user_info where id in (${user_ids})"
      val userMap = MyPhoenixUtil.queryList(sql).map{obj=>
        val id = obj.getString("ID")
        val user_level = obj.getString("USER_LEVEL")
        val birthday = obj.getString("BIRTHDAY")
        val gender = obj.getString("GENDER")
        val age_group = obj.getString("AGE_GROUP")
        val gender_name = obj.getString("GENDER_NAME")
        val userInfo = UserInfo(id,user_level,birthday,gender,age_group,gender_name)
        (userInfo.id.toLong,userInfo)
      }.toMap

      orders.foreach{order=>
        val userInfo = userMap.getOrElse(order.user_id,null)
        // 确保关联不上也不报错
        if(userInfo!=null){
          order.user_age_group = userInfo.age_group
          order.user_gender = userInfo.gender_name
        }
      }
      orders.iterator
    }

    // step5: 分批查phoenix，关联首单标记
    // 后面还需要继续使用，因此用mapPartition，如果后面不再使用，可以用foreachRDD
    val orderInfoWithFirstFlagDStream = orderWithUserInfoStream.mapPartitions{iter =>
      val orders = iter.toList
      if(orders.size>0){
        val uids = s"'${orders.map(_.user_id).mkString("','")}'"
        val sql = s"select user_id,if_consumed from ${phoenixTable} where user_id in (${uids})"
        val consumedUsers = MyPhoenixUtil.queryList(sql).map(_.getString("USER_ID"))
        orders.foreach{orderInfo =>
          // OrderInfo中user_id 为Long，而hbase的user_status中存储为varchar
          if(consumedUsers.contains(orderInfo.user_id.toString)){
            orderInfo.if_first_order = "0"
          }else{
            orderInfo.if_first_order = "1"
          }
        }
      }
      orders.iterator
    }

    val fixFirstFlagOrderStream = orderInfoWithFirstFlagDStream.map(order=>(order.user_id,order))
      .groupByKey()
      .flatMap{
        case (_,orderIter) =>
          // 5秒内同一个user_id 多次下单时，基于create_time升序排序，只将第一条标记为首单，其余标记为非首单
          var orders = orderIter.toList
          val order = orders(0)
          if(order.if_first_order=="1" && orders.size>1){
            orders = orders.sortWith((o1,o2) => o1.create_time < o2.create_time)
            for(i <- 1 until orders.size){
              orders(i).if_first_order = "0"
            }
          }
          orders
      }

    // step6: 将首单写入hbase,将order信息写入es
    fixFirstFlagOrderStream.foreachRDD{rdd =>
      // 缓存rdd(优化)
      rdd.cache()

      // 过滤出首单用户
      val firstOrderStream = rdd.filter(_.if_first_order=="1")

      // 首单用户状态维护到hbase
      firstOrderStream.map{order =>
            UserStatus(order.user_id.toString,order.if_first_order)
          }.saveToPhoenix(
            phoenixTable,
            Seq("USER_ID","IF_CONSUMED"),
            new Configuration(),
            Some(quorum)
          )

      // 写入order到es
      val bulkMap = rdd.collect().map{order =>
        val dt = order.create_date
        val key = s"${order.id.toString}:gmall_${dt.substring(0,4)}_order_info_${dt}"
        (key,order)
      }.toMap
      MyESUtil.bulkInsert(bulkMap)

      // 每个采集周期保存一次
      OffsetManagerUtil.saveOffsetToRedis(topic, groupId, offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
