package com.example.flink.common.func

import com.alibaba.fastjson.JSONObject
import com.example.flink.common.constant.{HbaseInfo, OperateType}
import com.example.flink.util.{DimUtil, PhoenixUtil}
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import java.sql.SQLException

/**
 * 输出到hbase,表存在于json中
 */
class PhoenixSinkFunc extends RichSinkFunction[JSONObject]{

  override def invoke(value: JSONObject, context: SinkFunction.Context): Unit = {
    val sinkTable = value.getString("sink_to")
    val data = value.getJSONObject("data")
    val opType = value.getString("type")
    val pk = value.getString("pk")
    val columns = data.keySet().toArray.mkString(",")
    val values = data.values().toArray.mkString("','")
    val sql = s"upsert into ${HbaseInfo.NAMESPACE}.${sinkTable} (${columns}) values('${values}')"
    try {
      PhoenixUtil.execute(sql)
      if(opType.equals(OperateType.UPDATE)){
        // TODO 当维度更新时，可以选择更新缓存或删除缓存, cache-aside 旁路缓存原理，数据变更，先失效缓存，写入数据库，然后在查库后写入缓存
        // 先更新缓存，然后写库，可能会导致数据不一致
        // updateDimCache(sinkTable,pk,data)
         deleteDimCache(sinkTable,pk,data)
      }
    } catch {
      case e:SQLException =>
        e.printStackTrace()
    }
  }

  /**
   * 更新redis中维度表相关缓存
   * @param table
   * @param data
   */
  def updateDimCache(table:String, pk:String, data:JSONObject): Unit ={
    val pkVal = data.getString(pk)
    DimUtil.updateCache(table,data.toJSONString,(pk,pkVal))
  }

  /**
   * 删除redis中维度表相关缓存
   * @param table
   * @param data
   */
  def deleteDimCache(table:String, pk:String, data:JSONObject): Unit ={
    val pkVal = data.getString(pk)
    DimUtil.deleteCache(table,(pk,pkVal))
  }

  override def close(): Unit = {
    PhoenixUtil.close()
  }


}
