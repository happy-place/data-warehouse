package com.example.flink.util

import java.util.Properties
import scala.tools.jline_embedded.internal.InputStreamReader

object PropertiesUtil {

  // 读取 properties 文件
  def load(propertiesName: String): Properties = {
    val props = new Properties()
     props.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName)))
    props
  }

}
