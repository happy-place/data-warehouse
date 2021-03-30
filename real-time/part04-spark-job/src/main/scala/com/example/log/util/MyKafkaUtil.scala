package com.example.log.util

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util.concurrent.Future
import java.util.Properties

object MyKafkaUtil {

  private val properties = MyPropertiesUtil.load("config.properties")
  private val brokerList = properties.getProperty("kafka.broker.list")

  var kafkaProducer:KafkaProducer[String,String] = null

  private val kafkaConsumerParam = collection.mutable.Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList, //用于初始化链接到集群的地址
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    //用于标识这个消费者属于哪个消费团体
    ConsumerConfig.GROUP_ID_CONFIG -> "dau_group",
    //latest自动重置偏移量为最新的偏移量"auto.offset.reset"
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
    //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
    //如果是false，会需要手动维护kafka偏移量"enable.auto.commit"
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
  )

  private val kafkaProducerParam = collection.mutable.Map(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList, //用于初始化链接到集群的地址
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer ",
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer ",
    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG -> (true: java.lang.Boolean), //允许幂等
    ProducerConfig.TRANSACTION_TIMEOUT_CONFIG -> (10 * 60 * 1000).toString
  )

  // 不传groupId
  def getKafkaStream(topic: String, ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaConsumerParam)
    )
    dStream
  }

  // 传groupId
  def getKafkaStream(topic: String, ssc: StreamingContext, groupId: String): InputDStream[ConsumerRecord[String, String]] = {
    kafkaConsumerParam("group.id") = groupId
    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaConsumerParam)
    )
    dStream
  }

  // 传groupId和currentOffsets
  def getKafkaStream(topic: String, ssc: StreamingContext, groupId: String, currentOffsets: Map[TopicPartition, Long]): InputDStream[ConsumerRecord[String, String]] = {
    kafkaConsumerParam("group.id") = groupId
    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaConsumerParam, currentOffsets),
    )
    dStream
  }

  def createKafkaProducer(): KafkaProducer[String,String] ={
    val props = new Properties()
    kafkaProducerParam.foreach(item =>props.setProperty(item._1,item._2.toString))
    kafkaProducer = new KafkaProducer[String, String](props)
    kafkaProducer
  }

  def send(topic:String,value:String): Future[RecordMetadata] ={
    if(kafkaProducer == null){
      createKafkaProducer()
    }
    kafkaProducer.send(new ProducerRecord[String, String](topic, value))
  }

  def send(topic:String,key:String,value:String): Future[RecordMetadata] ={
    if(kafkaProducer == null){
     createKafkaProducer()
    }
    kafkaProducer.send(new ProducerRecord[String,String](topic,key,value))
  }

}
