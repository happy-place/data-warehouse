package com.example.log.dim

import com.alibaba.fastjson.JSON
import com.example.log.model.UserInfo
import com.example.log.util.{MyKafkaUtil, MyPropertiesUtil, OffsetManagerUtil}
import org.apache.commons.lang.time.DateUtils
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.spark.toProductRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util.{Date}

/**
 * 将kafka gmall_maxwell_user_info 导入 phoenix gmall_user_info表
 */
object UserInfoApp {

  def main(args:Array[String]):Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))

    val topic = "ods_maxwell_user_info"
    val groupId = "user_info_app_group"

    val props = MyPropertiesUtil.load("config.properties")
    val quorum = props.getProperty("phoenix.quorum.url").substring(13)

    val phoenixTable = "gmall_user_info"

    // step1 读取offset
    val currentOffsets = OffsetManagerUtil.getOffsetFromRedis(topic, groupId)

    // step2 创建kafka 流
    var kafkaStream:DStream[ConsumerRecord[String,String]] = null
    if(currentOffsets!=null && currentOffsets.size>0){
      kafkaStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId, currentOffsets)
    }else{
      kafkaStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    // step3 提取kafka流offset
    var offsetRanges:Array[OffsetRange] = null
    val kafkaDStream = kafkaStream.transform{rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    // step4 转换为对象流
    val userInfoStream = kafkaDStream.map{record=>
      val userInfo = JSON.parseObject(record.value(),classOf[UserInfo])
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val birthday = sdf.parse(userInfo.birthday)
      val age = new Date().getYear - birthday.getYear

      if(age < 20){
        userInfo.age_group = "20岁以下"
      }else if(age>=20 && age < 30){
        userInfo.age_group = "20岁到30岁之间"
      }else{
        userInfo.age_group = "30岁以上"
      }

      if (userInfo.gender == "F"){
        userInfo.gender_name = "女"
      }else{
        userInfo.gender_name = "男"
      }

      userInfo
    }

    // step5 写入phoenix，并保存offset
    userInfoStream.foreachRDD{rdd =>
      rdd.saveToPhoenix(
        phoenixTable,
        Seq("ID","USER_LEVEL","BIRTHDAY","GENDER","AGE_GROUP","GENDER_NAME"),
        new Configuration(),
        Some(quorum)
      )
      OffsetManagerUtil.saveOffsetToRedis(topic, groupId, offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
  }


}
