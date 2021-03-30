package com.example.flink.util

import com.alibaba.fastjson.JSONObject
import com.example.flink.common.pool.{ClientPoolFactory, JDBCClientFactory}
import org.apache.commons.pool.impl.GenericObjectPool.Config
import org.junit.Test

import java.sql.Connection

class PoolFactoryTest {

  private def getPoolConfig():Config = {
    val props = PropertiesUtil.load("config.properties")
    val maxSize = props.getProperty("phoenix.pool.max-size").toInt
    val minIdle = props.getProperty("phoenix.pool.min-idle").toInt
    val maxIdle = props.getProperty("phoenix.pool.max-idle").toInt

    val poolConfig = new Config()
    poolConfig.testOnBorrow = true
    poolConfig.maxActive = maxSize
    poolConfig.maxIdle = maxIdle
    poolConfig
  }

  private def getClientFactory(): JDBCClientFactory ={
    new JDBCClientFactory("config.properties","config")
  }

  /**
   * 不管是否 close，要想不出错必须release
   */
  @Test
  def test1():Unit = {
    val poolConfig = getPoolConfig
    val clientFactory = getClientFactory()
    val pool = new ClientPoolFactory(clientFactory,poolConfig)
    var conn:Connection = null
    try {
      // 第一次借出
      conn = pool.get
      println(conn)
      conn.close()
      pool.release(conn)

      // 第二次借出
      conn = pool.get
      println(conn)
      conn.close()
      pool.release(conn)

    } catch {
      case e:Exception =>
        e.printStackTrace()
    } finally {
      pool.release(conn)
      pool.close()
    }
  }




}
