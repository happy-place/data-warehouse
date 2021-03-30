package com.example.flink.util

import org.apache.flink.api.common.serialization.{SimpleStringSchema}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}

import java.util.Properties

object KafkaUtil {

  private val properties = PropertiesUtil.load("config.properties")
  private val brokerList = properties.getProperty("kafka.broker.list")

  def createKafkaSource(topic:String, groupId:String): FlinkKafkaConsumer[String] ={
    val props = new Properties()
    props.put(ConsumerConfig.GROUP_ID_CONFIG,groupId)
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList)
    val consumer = new FlinkKafkaConsumer[String](topic,new SimpleStringSchema(),props)
    consumer
  }

  private val kafkaProducerParam = collection.mutable.Map(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList, //用于初始化链接到集群的地址
//    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
//    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG -> (true: java.lang.Boolean), //允许幂等
    ProducerConfig.TRANSACTION_TIMEOUT_CONFIG -> (15 * 60 * 1000).toString
  )

  def createKafkaSink(topic:String):FlinkKafkaProducer[String] = {
    val props = new Properties()
    kafkaProducerParam.foreach(item =>props.setProperty(item._1,item._2.toString))
    val producer = new FlinkKafkaProducer[String](topic,new SimpleStringSchema(),props)
    producer
  }

  def createKafkaSinkBySchema[T](serializeSchema:KafkaSerializationSchema[T]):FlinkKafkaProducer[T] = {
    val props = new Properties()
    kafkaProducerParam.foreach(item =>props.setProperty(item._1,item._2.toString))
    props.remove(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)
    props.remove(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)
    // TODO Semantic.EXACTLY 时 出异常 Exceeded checkpoint tolerable failure threshold.
    new FlinkKafkaProducer[T]("placeholder",serializeSchema,props,FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)
  }

  def createKafkaProducer(): KafkaProducer[String,String] ={
    val props = new Properties()
    kafkaProducerParam.foreach(item =>props.setProperty(item._1,item._2.toString))
    new KafkaProducer[String, String](props)
  }

  def getKafkaDDL(topic: String, groupId: String):String = {
    s""" 'connector' = 'kafka',
       |  'topic' = '${topic}',
       |  'properties.group.id' = '${groupId}',
       |  'scan.startup.mode' = 'earliest-offset',
       |  'properties.bootstrap.servers' = '${brokerList}',
       |  'format' = 'json'
       """.stripMargin
  }

}
