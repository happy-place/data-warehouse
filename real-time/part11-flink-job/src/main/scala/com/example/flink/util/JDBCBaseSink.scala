package com.example.flink.util

import com.example.flink.common.annotation.TransientSink
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.functions.sink.SinkFunction

import java.sql.PreparedStatement
import reflect.runtime.universe._

object JDBCBaseSink {

  def createSink[T:TypeTag](sql:String,batch:Int = 5,maxRetries:Int = 3,
                    driverName:String,jdbcUrl:String,user:String,password:String): SinkFunction[T] ={
    JdbcSink.sink(
      sql,
      new JdbcStatementBuilder[T]() {
        override def accept(ps: PreparedStatement, t: T): Unit = {
          val fields = t.getClass.getDeclaredFields
          var i = 1
          fields.foreach {field =>
            field.setAccessible(true)
            val annotatedFields = AnnotationUtil.getAnnotationFieldNames[T,TransientSink]()
            if (!annotatedFields.contains(field.getName)) {
              ps.setObject(i, field.get(t))
              i += 1
            }
          }
        }
      },
      JdbcExecutionOptions.builder()
        .withBatchSize(batch)
        .withMaxRetries(maxRetries)
        .build(),
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withDriverName(driverName)
        .withUrl(jdbcUrl)
        .withUsername(user)
        .withPassword(password)
        .build()
    )
  }

}
