package com.example.flink.common.func

import com.alibaba.fastjson.JSONObject
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

import java.lang

class KafkaSinkSerializeSchema extends KafkaSerializationSchema[JSONObject]{

  override def serialize(t: JSONObject, aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val topic = t.getString("sink_to")
    val bytes = t.getJSONObject("data").toJSONString.getBytes()
    new ProducerRecord(topic,bytes)
  }

}
