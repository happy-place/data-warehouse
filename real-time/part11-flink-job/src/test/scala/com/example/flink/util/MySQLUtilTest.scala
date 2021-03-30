package com.example.flink.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.example.flink.bean.{TableProcess}
import org.junit.Test

class MySQLUtilTest {

  @Test
  def test1(): Unit = {
    val sql = "select * from config.table_process"
    try {
      val processes = ConfigMysqlUtil.queryMany(sql, classOf[TableProcess])
      //    val processes = MySQLUtil.queryMany(sql, classOf[JSONObject])
      processes.foreach(println(_))
      processes.map { tp =>
        val sourceTable = tp.getSourceTable
        val operateType = tp.getOperateType
        val key = s"${sourceTable}:${operateType}"
        (key, tp)
      }.toMap
    } finally {
      ConfigMysqlUtil.close()
    }
  }

  @Test
  def test2(): Unit = {
    try {
      val sql = "select * from config.table_process"
      val process = ConfigMysqlUtil.queryOne(sql, classOf[TableProcess])
      //    val process = MySQLUtil.queryOne(sql, classOf[JSONObject])
      println(process)
    } finally {
      ConfigMysqlUtil.close()
    }
  }

  @Test
  def test3(): Unit = {
    try {
      val sql = "select * from config.table_process"
      //    val processes = MySQLUtil.queryMany(sql, classOf[TableProcess])
      val process = ConfigMysqlUtil.execute(sql)
      println(process)
    } finally {
      ConfigMysqlUtil.close()
    }
  }

}
