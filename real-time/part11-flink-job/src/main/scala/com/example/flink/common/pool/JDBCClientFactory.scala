package com.example.flink.common.pool

import java.sql.{Connection, DriverManager}
import java.util.Properties

/**
 * jdbc类型连接工厂
 * @param config
 * @param prefix
 */
class JDBCClientFactory()
  extends AbstractClientFactory[Connection]{

  private var driverName:String = null
  private var jdbcUrl:String = null
  private var username:String = null
  private var password:String = null

  def this(config:String,prefix:String) = {
    this()
    val props = new Properties()
    props.load(Thread.currentThread().getContextClassLoader.getResourceAsStream(config))
    driverName = props.getProperty(s"${prefix}.jdbc.driver")
    jdbcUrl = props.getProperty(s"${prefix}.jdbc.url")
    // TODO 不能使用contains
    if(props.containsKey(s"${prefix}.jdbc.user")){
      username = props.getProperty(s"${prefix}.jdbc.user")
      password = props.getProperty(s"${prefix}.jdbc.pass")
    }
  }

  override def makeObject(): AnyRef = {
    Class.forName(driverName)
    DriverManager.getConnection(jdbcUrl,username,password)
  }

  override def validateObject(o: Any): Boolean = {
    if(o.isInstanceOf[Connection]){
      val conn = o.asInstanceOf[Connection]
      conn.isValid(15)
    }else{
      false
    }
  }

}

