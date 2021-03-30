package com.example.flink.util

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import reflect.runtime.universe._

object ClickhouseUtil {

  private val props = PropertiesUtil.load("config.properties")
  private val driverName = props.getProperty("clickhouse.jdbc.driver")
  private val jdbcUrl = props.getProperty("clickhouse.jdbc.url")
  private var username:String = props.getOrDefault("clickhouse.jdbc.user",null).asInstanceOf[String]
  private var password:String = props.getOrDefault("clickhouse.jdbc.pass",null).asInstanceOf[String]

  def createSink[T:TypeTag](sql:String,batch:Int = 5,maxRetries:Int = 3): SinkFunction[T]  ={
    JDBCBaseSink.createSink[T](sql,batch,maxRetries,driverName,jdbcUrl,username,password)
  }

}
