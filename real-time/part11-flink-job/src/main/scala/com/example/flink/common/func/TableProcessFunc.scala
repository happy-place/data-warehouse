package com.example.flink.common.func

import com.alibaba.fastjson.JSONObject
import com.example.flink.bean.TableProcess
import com.example.flink.common.constant.{HbaseInfo, OperateType, SinkType}
import com.example.flink.util.{ConfigMysqlUtil, PhoenixUtil}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import java.sql.SQLException
import java.util.{Timer, TimerTask}
import scala.collection.mutable

/**
 * process api
 * maxwell采集到的ods_base_db topic 的binlog，在此处进行动态分流，依据mysql中配置，维度数据写入hbase，事实数据写入kafka,
 * 1.启动时获取mysql中数据分流配置，维护在内存中，过滤出维度相关表，维护在集合里，同时检测维度表是否存在于hbase中，不存在时需要创建；
 * 2.启动时设置定时任务，周期性从mysql读取数据分流配置，更新到内存
 * 3.没来一条记录，参照分流配置，执行相应输出操作，维度数据通过侧端输出流输出，事实数据放在主流中，最终需要写回到kafka
 *
 * 注：维度表创建需要使用特点命名空间 gmall_flink
 * hbase shell创建命名空间: create_namespace 'gmall_flink'
 * phoenix 创建schema映射命名空间: create schema if not exists gmall_flink;
 *
 * @param dimTag
 */

class TableProcessFunc(dimTag: OutputTag[JSONObject]) extends ProcessFunction[JSONObject, JSONObject] {
  private var log: Logger = null
  var timer: Timer = null
  val sql = "select * from config.table_process"
  var configMap: Map[String, TableProcess] = null
  var pkMap:mutable.HashMap[String,String] = null

  def refreshConfigMap(): Unit = {
    // 刷新configMap
    val processes = ConfigMysqlUtil.queryMany(sql, classOf[TableProcess])
    configMap = processes.map { tp =>
      val sourceTable = tp.getSourceTable
      val operateType = tp.getOperateType
      val key = s"${sourceTable}:${operateType}"
      (key, tp)
    }.toMap

    if (configMap == null || configMap.size == 0) {
      throw new RuntimeException("table_process config is empty")
    }

    configMap.foreach { tup =>
      val tableProcess = tup._2
      val sinkTable = tableProcess.getSinkTable
      if (SinkType.HBASE.equalsIgnoreCase(tableProcess.getSinkType)) {
        val notExisted = !pkMap.contains(sinkTable)
        if (notExisted) {
          checkTable(tableProcess)
        }
      }
    }
  }

  def checkTable(tableProcess: TableProcess): Unit = {
    var sinkPk = tableProcess.getSinkPk
    if (sinkPk == null || sinkPk.size == 0) {
      sinkPk = "id"
    }

    var sinkExtend = tableProcess.getSinkExtend
    if (sinkExtend == null || sinkExtend.size == 0) {
      sinkExtend = "SALT_BUCKETS = 3"
    }

    try {
      // hbase 维度表 未创建
      val sinkTable = tableProcess.getSinkTable
      pkMap.put(sinkTable,sinkPk)
      val sinkColumns = tableProcess.getSinkColumns.split(",")
      for (i <- 0 until sinkColumns.size) {
        val col = sinkColumns(i)
        if (sinkPk.equals(col)) {
          sinkColumns(i) = s"${col} varchar primary key"
        } else {
          sinkColumns(i) = s"info.${col} varchar"
        }
      }
      val columnExpress = sinkColumns.mkString(",\n")

      // 建表
      val ddl =
        s"""create table if not exists ${HbaseInfo.NAMESPACE}.${sinkTable} (
           |${columnExpress}
           |) ${sinkExtend}
           |""".stripMargin
      log.info(ddl)
      PhoenixUtil.execute(ddl)
    } catch {
      case e: SQLException =>
        e.printStackTrace()
        log.error(s"checkTable error ${e}")
    }
  }

  override def open(parameters: Configuration): Unit = {
    log = LoggerFactory.getLogger(this.getClass)
    pkMap = new mutable.HashMap[String,String]()

    // 查 mysql 初始化 configMap
    refreshConfigMap()

    // 设置定时器，周期性查询mysql，更新configMap，延迟5秒后启动定时器，以5秒间隔执行定时刷新任务
    timer = new Timer()
    timer.schedule(new TimerTask {
      override def run(): Unit = {
        log.info("refresh")
        refreshConfigMap()
      }
    }, 5000, 5000)
  }

  override def processElement(i: JSONObject, context: ProcessFunction[JSONObject, JSONObject]#Context,
                              collector: Collector[JSONObject]): Unit = {
    val result = new JSONObject()

    var opType = i.getString("type")

    if (opType.equals(OperateType.UPDATE) || opType.equals(OperateType.INSERT) || opType.equals(OperateType.BOOTSTRAP_INSERT)) {
      if (OperateType.BOOTSTRAP_INSERT.equals(opType)) {
        opType = "insert"
      }
      val table = i.getString("table")
      val key = s"${table}:${opType}"

      val data = i.getJSONObject("data")
      val tableProcess = configMap.getOrElse(key, null)
      if (tableProcess != null) {
        val columns = tableProcess.getSinkColumns.split(",")
        // 一边遍历一边删除需要借助迭代器
        val iter = data.entrySet().iterator()
        while(iter.hasNext){
          val entry = iter.next()
          if(!columns.contains(entry.getKey)){
            iter.remove()
          }
        }

        val sinkType = tableProcess.getSinkType
        val sinkTable = tableProcess.getSinkTable

        result.put("data", data)
        result.put("sink_to", tableProcess.getSinkTable)
        result.put("type", opType)

        if (SinkType.HBASE.equalsIgnoreCase(sinkType)) {
          result.put("pk", pkMap.get(sinkTable).get)
          context.output(this.dimTag, result)
        } else if (SinkType.KAFKA.equalsIgnoreCase(sinkType)) {
          collector.collect(result)
        }
      } else {
        log.warn(s"no process config for key(${key})")
      }
    }
  }

  override def close(): Unit = {
    timer.cancel()
    ConfigMysqlUtil.close()
    PhoenixUtil.close()
  }

}
