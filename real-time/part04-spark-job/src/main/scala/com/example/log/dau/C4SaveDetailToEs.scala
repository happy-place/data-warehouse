package com.example.log.dau

import com.alibaba.fastjson.JSON
import com.example.log.model.DauInfo
import com.example.log.util.{MyESUtil, MyKafkaUtil, MyRedisUtil}
import io.searchbox.core.{Bulk, Index}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util.Date

object C4SaveDetailToEs {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("dau-task").setMaster("local[3]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val topic = "rt_start_log"
    val groupId = "dau_group"

    // step1： 读取kafa 流
    val kafkaStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)

    val objStream = kafkaStream.map { record =>
      val nObject = JSON.parseObject(record.value())
      val ts = nObject.getString("ts").toLong
      val date = new Date(ts)
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val arr = sdf.format(date).split(" ")
      val dt = arr(0)
      val hr = arr(1).substring(0, 2)
      val mi = arr(1).substring(3, 5)
      nObject.put("dt", dt)
      nObject.put("hr", hr)
      nObject.put("mi", mi)
      nObject
    }

    // step2: 基于redis过滤出每天第一条启动日志
    val filtedStream = objStream.mapPartitions { iter =>
      val client = MyRedisUtil.getJedisClient
      val filtedIter = iter.filter { obj =>
        val mid = obj.getJSONObject("common").getString("mid")
        val dt = obj.getString("dt")
        // TODO dt 与 new Date() 对比，过滤非法数据
        val key = s"dau:${dt}"
        val isNew = client.sadd(key, mid) == 1L
        client.expire(key, 24 * 3600)
        // 注： expireAt 是指在指定时间点过期
        isNew
      }
      client.close()
      filtedIter
    }

    //    filtedStream.count().print()

    // step3：启动日志匹配插入ES
    // 方案1：mapPartitions + count 算子触发计算
    /*   val saveESFailedStream = filtedStream.mapPartitions{iter =>
         val client = MyESUtil.getClient
         val bulkBuilder = new Bulk.Builder()
         val dauMap = new scala.collection.mutable.HashMap[String,DauInfo]
         iter.foreach{ obj =>
           val dt = obj.getString("dt")
           val index = s"gmall${dt.substring(0,4)}_dau_info_${dt}"
           val commonObj = obj.getJSONObject("common")
           val mid = commonObj.getString("mid")

           val dauInfo = DauInfo(
             mid,
             commonObj.getString("uid"),
             commonObj.getString("ar"),
             commonObj.getString("ch"),
             commonObj.getString("vc"),
             obj.getString("dt"),
             obj.getString("hr"),
             obj.getString("mi"),
             obj.getString("ts").toLong
           )
           bulkBuilder.addAction(new Index.Builder(dauInfo).index(index).`type`("_doc").id(mid).build())
         }
         val bulkReq = bulkBuilder.build()
         val result = client.execute(bulkReq)
         client.close()
         result.getFailedItems().toArray.iterator
       }
       // 下面cont必须存在，否则不计算
       saveESFailedStream.count().print()*/

    // 方案2：DStream转换为RDD，然后使用foreachPartition处理
    // 无需count触发计算，但是需要将index混入key
    filtedStream.foreachRDD { rdd =>
      val bulkBuilder = new Bulk.Builder()
      val dauMap = new scala.collection.mutable.HashMap[String, DauInfo]
      rdd.foreachPartition { iter =>
        iter.foreach { obj =>
          val dt = obj.getString("dt")
          val index = s"gmall${dt.substring(0, 4)}_dau_info_${dt}"
          val commonObj = obj.getJSONObject("common")
          val mid = commonObj.getString("mid")
          val key = s"${mid}:${index}"

          val dauInfo = DauInfo(
            mid,
            commonObj.getString("uid"),
            commonObj.getString("ar"),
            commonObj.getString("ch"),
            commonObj.getString("vc"),
            obj.getString("dt"),
            obj.getString("hr"),
            obj.getString("mi"),
            obj.getString("ts").toLong
          )
          dauMap.put(key, dauInfo)
        }

        val result = MyESUtil.bulkInsert(dauMap.toMap)

        val failedItems = result.getFailedItems()
        if (failedItems.size() > 0) {
          println(s"${failedItems.size()}条记录保存ES失败")
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
