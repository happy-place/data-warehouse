package com.example.flink.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisUtil {
  var jedisPool: JedisPool = null
  val config = PropertiesUtil.load("config.properties")

  def getJedisClient: Jedis = {
    if (jedisPool == null) {
      RedisUtil.synchronized {
        if (jedisPool == null) {
          val host = config.getProperty("redis.host")
          val port = config.getProperty("redis.port")

          val jedisPoolConfig = new JedisPoolConfig()
          jedisPoolConfig.setMaxTotal(10) //最大连接数
          jedisPoolConfig.setMaxIdle(20) //最大空闲
          jedisPoolConfig.setMinIdle(20) //最小空闲
          jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
          jedisPoolConfig.setMaxWaitMillis(5000) //忙碌时等待时长 毫秒
          jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试
          jedisPoolConfig.setTestOnCreate(true) //每次获得连接的进行测试
          jedisPoolConfig.setTestOnReturn(true) //每次获得连接的进行测试
          jedisPoolConfig.setTestWhileIdle(true) //每次获得连接的进行测试

          val timeout = 5000
          val password = "pass"
          //      jedisPool=new JedisPool(jedisPoolConfig,host,port.toInt,timeout,password)
          jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt, timeout)
        }
      }
    }
    jedisPool.getResource
  }

}
