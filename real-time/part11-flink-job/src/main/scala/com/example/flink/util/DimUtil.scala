package com.example.flink.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.example.flink.common.constant.HbaseInfo
import redis.clients.jedis.Jedis

object DimUtil {

  private def getWhere(params: Tuple2[String, Any]*): String = {
    // rowkey 全部按字符串处理
    val conditions = params.map {
      case (column, value) => s"${column} = '${value}'"
    }.toList.mkString(" and ")
    var where: String = ""
    if (conditions.size > 0) {
      where = s"where ${conditions}"
    }
    where
  }

  private def getKey(table: String, params: Tuple2[String, Any]*): String = {
    val conditions = params.map {
      case (_, value) => s"${value}"
    }.toList.mkString(":")
    s"${HbaseInfo.NAMESPACE}.${table}:${conditions}"
  }

  def queryOneWithNoCache[T](table: String, clazz: Class[T], params: Tuple2[String, Any]*): T = {
    val where = getWhere(params: _*)
    val sql = s"select * from ${HbaseInfo.NAMESPACE}.${table} ${where}".toUpperCase()
    val result = PhoenixUtil.queryOne(sql, clazz, false)
    if (result != null) {
      val jsonStr = JSON.toJSONString(result, new SerializeConfig(true))
      if (jsonStr != null && !jsonStr.equals("{}")) {
        val key = getKey(table, params: _*)
        var redisConn: Jedis = null
        try {
          redisConn = RedisUtil.getJedisClient
          redisConn.setex(key, 24 * 3600, jsonStr)
        } finally {
          if (redisConn != null) {
            redisConn.close()
          }
        }
      }
    }
    result
  }

  def queryOneWithCache[T](table: String, clazz: Class[T], params: Tuple2[String, Any]*): T = {
    val key = getKey(table, params: _*)
    var jsonStr: String = null
    var redisConn: Jedis = null
    try {
      redisConn = RedisUtil.getJedisClient
      jsonStr = redisConn.get(key)
    } finally {
      if (redisConn != null) {
        redisConn.close()
      }
    }
    if (jsonStr != null && !jsonStr.equals("{}")) {
      JSON.parseObject(jsonStr, clazz)
    } else {
      queryOneWithNoCache(table, clazz, params: _*)
    }
  }

  def deleteCache(table: String, params: Tuple2[String, Any]*): Unit = {
    val key = getKey(table, params: _*)
    var redisConn: Jedis = null
    try {
      redisConn = RedisUtil.getJedisClient
      redisConn.del(key)
    } finally {
      if (redisConn != null) {
        redisConn.close()
      }
    }
  }

  def updateCache(table: String, value: String, params: Tuple2[String, Any]*): Unit = {
    val key = getKey(table, params: _*)
    var redisConn: Jedis = null
    try {
      redisConn = RedisUtil.getJedisClient
      val isExisted = redisConn.exists(key)
      if (isExisted) {
        redisConn.setex(key, 24 * 3600, value)
      }
    } finally {
      if (redisConn != null) {
        redisConn.close()
      }
    }
  }

  def close(): Unit ={
    PhoenixUtil.close()
  }

}
