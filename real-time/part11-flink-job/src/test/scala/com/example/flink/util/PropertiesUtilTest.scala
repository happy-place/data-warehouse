package com.example.flink.util

import com.example.flink.util.PropertiesUtil.load
import org.junit.Test

class PropertiesUtilTest {

  @Test
  def test1(): Unit = {
    val properties = load("config.properties")
    println(properties.getProperty("kafka.broker.list"))
  }

}
