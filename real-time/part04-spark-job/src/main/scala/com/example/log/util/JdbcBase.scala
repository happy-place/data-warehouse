package com.example.log.util

import com.alibaba.fastjson.JSONObject

import java.sql.{Connection, SQLException}
import scala.collection.mutable.ListBuffer

abstract class JdbcBase {

  def getConnect(): Connection

  def queryList(sql:String): List[JSONObject] ={
    val result = new ListBuffer[JSONObject]
    val conn = getConnect()
    val statement = conn.createStatement()
    try {
      val rs = statement.executeQuery(sql)
      val metaData = rs.getMetaData
      while (rs.next()) {
        val row = new JSONObject()
        val count = metaData.getColumnCount
        for (i <- 1 to count) {
          row.put(metaData.getColumnName(i), rs.getObject(i))
        }
        result.append(row)
      }
    } catch {
      case e:SQLException =>
        e.printStackTrace()
        throw e
    } finally {
      statement.close()
      conn.close()
    }
    result.toList
  }

}
