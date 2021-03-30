package com.example.flink.common.func

import com.alibaba.fastjson.JSONObject
import com.example.flink.util.KafkaUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * 输出到kafka,表存在于json中
 */
class KafkaSinkFunc extends RichSinkFunction[JSONObject]{

  var kafkaProducer:KafkaProducer[String,String] = null

  override def open(parameters: Configuration): Unit = {
    kafkaProducer = KafkaUtil.createKafkaProducer()
  }

  override def invoke(value: JSONObject, context: SinkFunction.Context): Unit = {
    val topic = value.getString("sink_to")
    val data = value.getJSONObject("data").toJSONString
    kafkaProducer.send(new ProducerRecord[String, String](topic, data))
  }

  override def close(): Unit = {
    kafkaProducer.close()
  }

}
