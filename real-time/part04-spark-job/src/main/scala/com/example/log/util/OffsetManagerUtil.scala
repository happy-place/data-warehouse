package com.example.log.util

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.OffsetRange

import java.time.Duration
import scala.collection.JavaConversions._

object OffsetManagerUtil {

  private val properties = MyPropertiesUtil.load("config.properties")
  private val brokerList = properties.getProperty("kafka.broker.list")

  private val kafkaConsumerParam = collection.mutable.Map(
    "bootstrap.servers" -> brokerList, //用于初始化链接到集群的地址
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    //用于标识这个消费者属于哪个消费团体
    "group.id" -> "dau_group",
    //latest自动重置偏移量为最新的偏移量
    "auto.offset.reset" -> "latest",
    //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
    //如果是false，会需要手动维护kafka偏移量
    "enable.auto.commit" -> (false: java.lang.Boolean),
    "request.timeout.ms" -> "10000",
  )

  private def filterWithKafkaMeta(topic: String,groupId: String, otherOffsets: collection.mutable.Map[TopicPartition, Long]):
  (Boolean,collection.mutable.Map[TopicPartition, Long]) = {
    if(otherOffsets != null && otherOffsets.size > 0){
      kafkaConsumerParam.put("group.id",groupId)
      val kafkaConsumer = new KafkaConsumer(kafkaConsumerParam)
      val topicAndPartitions = kafkaConsumer.partitionsFor(topic).map(partitionInfo => new TopicPartition(topic, partitionInfo.partition()))
      val beginningOffsets = kafkaConsumer.beginningOffsets(topicAndPartitions,Duration.ofSeconds(60))
      val endOffsets = kafkaConsumer.endOffsets(topicAndPartitions,Duration.ofSeconds(60))

      val kafkaOffsets = scala.collection.mutable.Map[TopicPartition, Long]()
      endOffsets.foreach{tup=>
        kafkaOffsets.put(tup._1,tup._2)
      }

      if(kafkaOffsets.size != otherOffsets.size){
        // topic被重建，需要删除other相关数据
        (false,kafkaOffsets)
      }else{
        // 分区数相同，kafka endOffsets 小于 other 的offset，以 kafka 为准，需要删除other相关数据
        kafkaOffsets.foreach{tup =>
          val topicAndPartition = tup._1
          val redisOffset = otherOffsets.get(topicAndPartition).get
          val kafkaBeginOffset = beginningOffsets.get(topicAndPartition)
          val kafkaEndOffset = endOffsets.get(topicAndPartition)
          if(redisOffset < kafkaBeginOffset || redisOffset > kafkaEndOffset){
            return (false,kafkaOffsets)
          }
        }
        // 分区数相同，kafka endOffsets 大于或等于 other offset，以 other为准
        (true,otherOffsets)
      }
    }else{
      (false,otherOffsets)
    }
  }

  def getOffsetFromRedis(topic: String, groupId: String): Map[TopicPartition, Long] = {
    val key = s"${topic}:${groupId}"
    val redisOffsets = scala.collection.mutable.Map[TopicPartition, Long]()
    val client = MyRedisUtil.getJedisClient
    client.hgetAll(key).forEach { (key, value) =>
      redisOffsets.put(new TopicPartition(topic, key.toInt), value.toLong)
    }
//    val (toUse,currentOffsets) = filterWithKafkaMeta(topic,groupId, redisOffsets)
//    if(!toUse){
//      client.del(key)
//    }
    redisOffsets.toMap
  }

  // 有幂等保证，才能将写offset可写结果分开
  def saveOffsetToRedis(topic: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    if (offsetRanges != null && offsetRanges.size > 0) {
      val key = s"${topic}:${groupId}"
      val map = offsetRanges.map { offsetRange =>
        val partition = offsetRange.partition
        val offset = offsetRange.untilOffset
        (partition.toString, offset.toString)
      }.toMap
      val client = MyRedisUtil.getJedisClient
      client.hmset(key, map)
      client.close()
    }
  }

  /**
   * saveOffsetToMysql 通常在数据消费的相同事务中被保存
   * @param topic
   * @param groupId
   * @return
   */
  def getOffsetFromMySql(topic: String, groupId: String): Map[TopicPartition, Long] = {
    val mysqlOffsets = scala.collection.mutable.Map[TopicPartition, Long]()
    val sql = s"select topic,group_id,partition_id,`offset` from kafka_offset where topic='${topic}' and group_id='${groupId}'"
    MyMysqlUtil.queryList(sql).foreach{obj =>
      mysqlOffsets.put(new TopicPartition(topic, obj.getInteger("partition_id")), obj.getLong("offset"))
    }
    mysqlOffsets.toMap
  }

}
