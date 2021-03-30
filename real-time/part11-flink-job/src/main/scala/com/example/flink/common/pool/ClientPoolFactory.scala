package com.example.flink.common.pool

import org.apache.commons.pool.impl.GenericObjectPool
import org.apache.commons.pool.impl.GenericObjectPool.Config

import java.util.Properties

/**
 * 创建连接对象池子的工厂，已经完全实现，直接包裹创建连接对象的工厂就可以使用，具备池子管理对象能力
 * @param factory
 * @tparam T
 */
class ClientPoolFactory[T <: AutoCloseable](factory:AbstractClientFactory[T]){
  private val pool:GenericObjectPool = new GenericObjectPool(factory)

  def this(factory:AbstractClientFactory[T],config: Config) = {
    this(factory)
    if(config!=null){
      pool.setConfig(config)
    }
  }

  def this(factory:AbstractClientFactory[T],configFile:String,prefix:String) = {
    this(factory)
    if(configFile!=null){
      val poolConfig = initPoolConfig(configFile,prefix)
      pool.setConfig(poolConfig)
    }
  }

  private def initPoolConfig(configFile:String,prefix:String): Config ={
    val props = new Properties()
    props.load(Thread.currentThread().getContextClassLoader.getResourceAsStream(configFile))
    val poolConfig = new Config()

    var key = s"${prefix}.maxIdle"
    if(props.containsKey(key)){
      poolConfig.maxIdle = props.getProperty(key).trim.toInt
    }
    key = s"${prefix}.maxActive"
    if(props.containsKey(key)){
      poolConfig.maxActive = props.getProperty(key).trim.toInt
    }
    key = s"${prefix}.maxWait"
    if(props.containsKey(key)){
      poolConfig.maxWait = props.getProperty(key).trim.toLong
    }
    key = s"${prefix}.whenExhaustedAction"
    if(props.containsKey(key)){
      poolConfig.whenExhaustedAction = props.getProperty(key).trim.toByte
    }
    key = s"${prefix}.testOnBorrow"
    if(props.containsKey(key)){
      poolConfig.testOnBorrow = props.getProperty(key).trim.toBoolean
    }
    key = s"${prefix}.testOnReturn"
    if(props.containsKey(key)){
      poolConfig.testOnReturn = props.getProperty(key).trim.toBoolean
    }
    key = s"${prefix}.testWhileIdle"
    if(props.containsKey(key)){
      poolConfig.testWhileIdle = props.getProperty(key).trim.toBoolean
    }
    key = s"${prefix}.timeBetweenEvictionRunsMillis"
    if(props.containsKey(key)){
      poolConfig.timeBetweenEvictionRunsMillis = props.getProperty(key).trim.toLong
    }
    key = s"${prefix}.numTestsPerEvictionRun"
    if(props.containsKey(key)){
      poolConfig.numTestsPerEvictionRun = props.getProperty(key).trim.toInt
    }
    key = s"${prefix}.minEvictableIdleTimeMillis"
    if(props.containsKey(key)){
      poolConfig.minEvictableIdleTimeMillis = props.getProperty(key).trim.toLong
    }

    poolConfig
  }


  @throws[Exception]
  def get: T = pool.borrowObject.asInstanceOf[T]

  /**
   * 不管是否 close，要想不出错必须release
   */
  @throws[Exception]
  def release(client: T): Unit = {
    try {
      pool.returnObject(client)
    } catch {
      case e: Exception =>
        if (client != null) {
          try {
            client
          } catch {
            case ex: Exception =>
              ex.printStackTrace()
              throw ex
          }
        }
    }
  }

  @throws[Exception]
  def close(): Unit ={
    if(pool!=null){
      pool.close()
    }
  }

}


