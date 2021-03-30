package com.example.flink.common.func

import com.alibaba.fastjson.JSONObject
import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.slf4j.{Logger, LoggerFactory}

import java.text.SimpleDateFormat
//import java.time.LocalDateTime
//import java.time.ZoneOffset
//import java.time.format.DateTimeFormatter
import java.util.Date

class FirstPageFilterFunc extends RichFilterFunction[JSONObject]{
  // JobManager中声明
  private var log: Logger = null

  var firstVisitState:ValueState[String] = null
  var sdf:SimpleDateFormat = null // format() 中calander.setTime() 存在线程安全问题
//  var dtf: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

  override def open(parameters: Configuration): Unit = {
    // taskManager中实例化，如此就不存在序列化问题
    log = LoggerFactory.getLogger(this.getClass)
    sdf = new SimpleDateFormat("yyyyMMdd")
//    dtf = DateTimeFormatter.ofPattern("yyyyMMdd")
    val stateDesc = new ValueStateDescriptor[String]("first_visit_date", classOf[String])
    val config = StateTtlConfig.newBuilder(Time.days(1))
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 创建和写入时候允许更新
      .build()
    stateDesc.enableTimeToLive(config)
    firstVisitState = getRuntimeContext.getState(stateDesc)
  }

  override def filter(in: JSONObject): Boolean = {
    val lastPageId = in.getJSONObject("page").getString("last_page_id")
    if(lastPageId!=null && lastPageId.size>0){ // 不是用户访问的首个页面，直接去掉
//      log.debug("非首个页面")
      return false
    }

    val savedDate = firstVisitState.value()
    val logDate = sdf.format(new Date(in.getLong("ts")))
//    val logDate = LocalDateTime.ofEpochSecond(in.getLong("ts") / 1000L, 0, ZoneOffset.ofHours(8)).format(dtf)
    if(savedDate != null && savedDate.size>0 && savedDate.equals(logDate)){
      // savedDate 非空判断解决是否保存过状态，date是否一致解决跨天访问(注意需要设置状态更新时，ttl也需要更新)
//      println("首个页面非首次访问")
      false
    }else{
//      log.debug("首个页面首次访问")
      firstVisitState.update(logDate)
      true
    }

  }
}
