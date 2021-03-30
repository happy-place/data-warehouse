package com.example.log.dim

import com.alibaba.fastjson.JSON
import com.example.log.model.{BaseCategory3, BaseTrademark, SkuInfo, SpuInfo}
import com.example.log.util.{MyKafkaUtil, MyPhoenixUtil, MyPropertiesUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.spark.toProductRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object SkuInfoExpandApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(conf, Seconds(5))

    val topic = "ods_maxwell_sku_info"
    val groupId = "sku_info_expand_app_group"
    val phoenixTable = "gmall_sku_info_expand"

    val props = MyPropertiesUtil.load("config.properties")
    val quorum = props.getProperty("phoenix.quorum.url").substring(13)

    // 获取offset
    val currentOffsets = OffsetManagerUtil.getOffsetFromRedis(topic,groupId)

    // 创建kafka流
    var kafkaStream:DStream[ConsumerRecord[String,String]] = null
    if(currentOffsets!=null&&currentOffsets.size>0){
      kafkaStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId, currentOffsets)
    }else{
      kafkaStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    // 提取 offset
    var offsetRanges:Array[OffsetRange] = null
    val extractedKafkaStream = kafkaStream.transform{rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    // 转换为对象流
    val skuInfoStream = extractedKafkaStream.map{record =>
      JSON.parseObject(record.value(),classOf[SkuInfo])
    }

    // 以rdd为单位出发关联查询，关联品牌、品类、spu信息 （每个采集周期广播一次，让各分区处理一次）
    val skuInfoStreamWithFullInfo = skuInfoStream.transform{rdd=>
      val list = ListBuffer[Map[String,Any]]()
      val trademarkSql = s"select * from gmall_base_trademark"
      val trademarkMap = MyPhoenixUtil.queryList(trademarkSql).map{obj=>
        val trademark = BaseTrademark(obj.getString("ID"),obj.getString("TM_NAME"))
        (trademark.tm_id,trademark)
      }.toMap

      list.append(trademarkMap)

      val categorySql = s"select * from gmall_base_category3"
      val categoryMap = MyPhoenixUtil.queryList(categorySql).map{obj=>
        val category = BaseCategory3(obj.getString("ID"),obj.getString("NAME"),obj.getString("CATEGORY2_ID"))
        (category.id,category)
      }.toMap
      list.append(categoryMap)

      val spuSql = s"select * from gmall_spu_info"
      val spuMap = MyPhoenixUtil.queryList(spuSql).map{obj=>
        val spu = SpuInfo(obj.getString("ID"),obj.getString("SPU_NAME"))
        (spu.id,spu)
      }.toMap

      list.append(spuMap)

      val listBC = ssc.sparkContext.broadcast(list)

      rdd.mapPartitions{iter =>
        val list = listBC.value
        val trademarkMap = list(0).asInstanceOf[Map[String,BaseTrademark]]
        val categoryMap = list(1).asInstanceOf[Map[String,BaseCategory3]]
        val spuMap = list(2).asInstanceOf[Map[String,SpuInfo]]
        iter.map{ skuInfo =>
          val trademark = trademarkMap.getOrElse(skuInfo.tm_id,null)
          if(trademark!=null){
            skuInfo.tm_name = trademark.tm_name
          }
          val category3 = categoryMap.getOrElse(skuInfo.category3_id,null)
          if(category3!=null){
            skuInfo.category3_name = category3.name
          }
          val spuInfo = spuMap.getOrElse(skuInfo.spu_id,null)
          if(trademark!=null){
            skuInfo.spu_name = spuInfo.spu_name
          }
          skuInfo
        }
      }
    }

    // 以rdd为单位，写入phoenix，并在每个采集周期保存offset
    skuInfoStreamWithFullInfo.foreachRDD{rdd=>
      rdd.saveToPhoenix(
        phoenixTable,
        Seq("ID","SPU_ID","PRICE","SKU_NAME","TM_ID","CATEGORY3_ID","CREATE_TIME","CATEGORY3_NAME","SPU_NAME","TM_NAME"),
        new Configuration(),
        Some(quorum)
      )

      OffsetManagerUtil.saveOffsetToRedis(topic,groupId, offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
