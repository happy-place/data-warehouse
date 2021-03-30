package com.example.log.util

import com.alibaba.fastjson.JSONObject

import java.sql.{Connection, DriverManager, SQLException}
import java.util.Properties
import scala.collection.mutable.ListBuffer

object MyPhoenixUtil extends JdbcBase{

  def getConnect(): Connection ={
    val conf = new Properties()
    conf.setProperty("phoenix.schema.isNamespaceMappingEnabled","true")
    val props = MyPropertiesUtil.load("config.properties")
    Class.forName(props.getProperty("phoenix.driver.class"))
    DriverManager.getConnection(props.getProperty("phoenix.quorum.url"),conf)
  }

//  def queryList(sql:String): List[JSONObject] ={
//    val result = new ListBuffer[JSONObject]
//    val conn = getConnect()
//    val statement = conn.createStatement()
//    try {
//      val rs = statement.executeQuery(sql)
//      val metaData = rs.getMetaData
//      while (rs.next()) {
//        val row = new JSONObject()
//        val count = metaData.getColumnCount
//        for (i <- 1 to count) {
//          row.put(metaData.getColumnName(i), rs.getObject(i))
//        }
//        result.append(row)
//      }
//    } catch {
//      case e:SQLException =>
//        e.printStackTrace()
//        throw e
//    } finally {
//      statement.close()
//      conn.close()
//    }
//    result.toList
//  }

}
