package com.example.flink.util

import com.alibaba.fastjson.JSONObject
import com.example.flink.common.pool.{ClientPoolFactory, JDBCClientFactory}
import com.google.common.base.CaseFormat
import org.apache.commons.beanutils.BeanUtils

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}
import scala.collection.mutable.ListBuffer

/**
 * JDBC类型工具类，具备CRUD能力，池化技术管理连接
 * @param configFile
 * @param prefix
 */
class JDBCBaseUtil(configFile:String, prefix:String) {

  private val clientFactory = new JDBCClientFactory(configFile,prefix)
  private val pool:ClientPoolFactory[Connection] = new ClientPoolFactory(clientFactory,configFile,prefix)

  def queryOne[T](sql:String,clazz:Class[T], underScoreToCamel:Boolean=true):T = {
    val many = queryMany(sql, clazz, underScoreToCamel)
    if(many!=null && many.size>0){
      many(0)
    }else{
      clazz.newInstance()
    }
  }

  def queryMany[T](sql:String, clazz: Class[T], underScoreToCamel:Boolean=true):List[T] = {
    val result = ListBuffer[T]()
    var conn:Connection = null
    var ps:PreparedStatement = null
    var rs: ResultSet = null
    try {
      conn = pool.get
      ps = conn.prepareStatement(sql)
      rs = ps.executeQuery()
      if (rs != null) {
        val meta = rs.getMetaData
        val count = meta.getColumnCount
        while (rs.next()) {
          val t = clazz.newInstance()
//          val t = new JSONObject()
          for (i <- 1 to count) {
            var name = meta.getColumnName(i)
            if (underScoreToCamel) {
              name = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name)
            }
            val data = rs.getObject(i)
            BeanUtils.setProperty(t, name, data)
//            t.put(name,data)
          }
//          result.append(t.toJavaObject(clazz))
          result.append(t)
        }
      }
      result.toList
    }finally {
      if(rs!=null){
        try {
          rs.close()
        } catch {
          case e:SQLException =>
            e.printStackTrace()
        }
      }
      if(ps!=null){
        try {
          ps.close()
        } catch {
          case e:SQLException =>
            e.printStackTrace()
        }
      }
      if(conn!=null){
        try {
          pool.release(conn)
        } catch {
          case e:SQLException =>
            e.printStackTrace()
        }
      }
    }
  }

  def execute(sql:String):Boolean = {
    var conn:Connection = null
    var ps:PreparedStatement = null
    try {
      conn = pool.get
      ps = conn.prepareStatement(sql)
      val result = ps.execute() // 返回 boolean
      // phoenix 默认不自动提交
      conn.commit()
      result
    }  finally {
      if(ps!=null){
        try {
          ps.close()
        } catch {
          case e:SQLException =>
            e.printStackTrace()
        }
      }
      if(conn!=null){
        try {
          pool.release(conn)
        } catch {
          case e:SQLException =>
            e.printStackTrace()
        }
      }
    }
  }

  /**
   * 先释放池子中的连接，然后关闭池子
   */
  def close(): Unit ={
    pool.close()
  }
  
}


