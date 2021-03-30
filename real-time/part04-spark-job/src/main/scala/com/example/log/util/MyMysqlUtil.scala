package com.example.log.util

import com.alibaba.druid.pool.DruidDataSourceFactory
import java.sql.{Connection, PreparedStatement, SQLException}
import javax.sql.DataSource

object MyMysqlUtil extends JdbcBase{

  private var dataSource:DataSource = null;

  def getConnect(): Connection ={
    if(dataSource == null){
      val config = MyPropertiesUtil.load("config.properties")
      dataSource = DruidDataSourceFactory.createDataSource(config);
    }
    dataSource.getConnection()
  }

  def main(args: Array[String]): Unit = {
    var conn:Connection = null
    var ps: PreparedStatement = null
    try {
      conn = getConnect
      conn.setAutoCommit(false)
      ps = conn.prepareStatement("insert into student(name,age) value(?,?)")
      ps.setString(1,"tom")
      ps.setInt(2,20)
      ps.execute()
//      throw new SQLException("test")
      conn.commit()
    } catch {
      case e:SQLException =>
        e.printStackTrace()
        conn.rollback()
    } finally {
      ps.close()
      conn.close()
    }
  }

}
