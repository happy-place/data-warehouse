package com.example.log.dau

import com.alibaba.fastjson.JSON
import com.example.log.util.{MyKafkaUtil, MyRedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util.Date

object C3DistinctByRedis {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("dau-task").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val topic = "rt_start_log"
    val groupId = "dau_group"

    // step1: 读取 kafka 指定topic消息
    val kafkaStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)

    // step2: 添加新字段
    val objStream = kafkaStream.map { record =>
      val obj = JSON.parseObject(record.value())
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val date = new Date(obj.getString("ts").toLong)
      val arr = sdf.format(date).split(" ")
      val dt = arr(0)
      val hr = arr(1).substring(0, 2)
      obj.put("dt", dt)
      obj.put("hr", hr)
      obj
    }

    // objStream.print()

    // step3: 使用Redis去重，过滤出每天的提一条启动日志
    // 方案1：来一条处理一条（访问redis过于频繁，存在性能问题）
    // 插入 redis set 数据结构，如果已经存在就返回 0L, 否则返回1L
    // 凡是返回1L的都是第一条启动日志，需要进入后面的计算
    // 存在问题

    /*
    val filteredStartStream = objStream.filter { obj =>
      val mid = obj.getJSONObject("common").getString("mid")
      val dt = obj.getString("dt")
      val client = MyRedisUtil.getJedisClient
      val key = s"dau:${dt}"
      val isNew = client.sadd(key, mid) == 1L
      // 设置key过期时间，自动清除
      client.expire(key,24*3600*1000)
      // 注：此处需要关闭资源否则jedis pool 不够用
      client.close()
      isNew
    }

    filteredStartStream.count().print()
    */

    // 方案2：以partition为单位处理一次访问redis一次
    val filteredStartStream = objStream.mapPartitions { iter =>
      val client = MyRedisUtil.getJedisClient
      val filtedIter = iter.filter { obj =>
        val mid = obj.getJSONObject("common").getString("mid")
        val dt = obj.getString("dt")
        // TODO 此处还可以对dt 与 new Date() 进行比对，排查非法日期
        val key = s"dau:${dt}"
        val isNew = client.sadd(key, mid) == 1L
        client.expire(key, 24 * 3600) // expireAt 在指定时间点过期
        isNew
      }
      client.close()
      filtedIter
    }

    filteredStartStream.count().print()

    ssc.start()
    ssc.awaitTermination()
  }

}
