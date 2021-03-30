package com.example.log.util

import java.io.InputStreamReader
import java.util.Properties

object MyPropertiesUtil {

  def main(args: Array[String]): Unit = {
    val properties = load("config.properties")
    println(properties.getProperty("kafka.broker.list"))
  }

  // 读取 properties 文件
  def load(propertiesName: String): Properties = {
    val props = new Properties()
    props.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName)))
    props
  }

}
