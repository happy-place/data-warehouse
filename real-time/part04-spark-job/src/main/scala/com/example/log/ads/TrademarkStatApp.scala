package com.example.log.ads

import com.alibaba.fastjson.JSON
import com.example.log.model.OrderWide
import com.example.log.util.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

object TrademarkStatApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(conf, Seconds(5))

    val topic = "dwd_order_wide"
    val groupId = "trademark_stat_app_group"

    val currentOffsets = OffsetManagerUtil.getOffsetFromMySql(topic, groupId)

    var kafkaStream:DStream[ConsumerRecord[String,String]] = null
    if(currentOffsets!=null && currentOffsets.size>0){
      kafkaStream = MyKafkaUtil.getKafkaStream(topic,ssc, groupId, currentOffsets)
    }else{
      kafkaStream = MyKafkaUtil.getKafkaStream(topic,ssc, groupId)
    }

    var offsetRanges = new ListBuffer[Seq[Any]]
    val orderWideStream = kafkaStream.transform{rdd =>
      rdd.asInstanceOf[HasOffsetRanges].offsetRanges.map(or=>
        offsetRanges.append(Seq(groupId,or.topic,or.partition,or.untilOffset))
      )
      rdd
    }.map(record => JSON.parseObject(record.value(),classOf[OrderWide]))

    val statStream = orderWideStream.map(o => (s"${o.tm_id}:${o.tm_name}",o.final_detail_amount))
      .reduceByKey(_+_)


    statStream.foreachRDD{rdd =>
      val tmSumArr:Array[(String,Double)] = rdd.collect()
      if(tmSumArr!=null && tmSumArr.size >0){
        DBs.setup()
        DB.localTx{
          implicit session =>
            val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val date = sdf.format(new Date())
            val args = new ListBuffer[Seq[Any]]
            tmSumArr.foreach{row =>
              val temp = row._1.split(":")
              val tm_id = temp(0)
              val tm_name = temp(1)
              val amount = row._2
              args.append(Seq(date,tm_id,tm_name,amount))
            }
            val saveResultSql = "insert into trademark_amount_stat(stat_time,trademark_id,trademark_name,amount) values(?,?,?,?)"
            SQL(saveResultSql).batch(args:_*).apply()

            val saveOffsetSql = "replace into kafka_offset(group_id,topic,partition_id,`offset`) values(?,?,?,?)"
            SQL(saveOffsetSql).batch(offsetRanges:_*).apply()
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
