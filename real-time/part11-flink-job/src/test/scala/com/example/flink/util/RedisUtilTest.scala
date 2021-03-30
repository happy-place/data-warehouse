package com.example.flink.util

import com.example.flink.util.RedisUtil.getJedisClient
import org.junit.Test

class RedisUtilTest {

  @Test
  def test1(): Unit = {
    val jedisClient = getJedisClient
    println(jedisClient.ping())
    jedisClient.close()
  }

}
