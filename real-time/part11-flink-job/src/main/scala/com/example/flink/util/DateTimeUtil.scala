package com.example.flink.util

import java.time.{LocalDate, LocalDateTime, OffsetDateTime, ZoneId, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.Date

/**
 * 时间日期相关工具类
 */



object DateTimeUtil {

  private val zoneid: ZoneId = ZoneId.systemDefault()
  private val zoneOffset: ZoneOffset= OffsetDateTime.now(zoneid).getOffset()

  def toStr(datetime:Date,pattern:String): String ={
    val formatter = DateTimeFormatter.ofPattern(pattern)
    LocalDateTime.ofInstant(datetime.toInstant,zoneid).format(formatter)
  }

  /**
   *
   * @param timestamp like 1616730764657
   * @param pattern
   * @return
   */
  def toStr(timestamp:Long,pattern:String): String ={
    val formatter = DateTimeFormatter.ofPattern(pattern)
    LocalDateTime.ofInstant(new Date(timestamp).toInstant,zoneid).format(formatter)
  }

  def toTs(datetime:Date): Long ={
    datetime.getTime
  }

  /**
   * @param str
   * @param pattern
   * @return like 1616730764657
   */
  def toTs(str:String,pattern:String): Long ={
    val formatter = DateTimeFormatter.ofPattern(pattern)
    var localdatetime:LocalDateTime = null
    if(pattern.contains("HH") || pattern.contains("mm") || pattern.contains("ss")){
      localdatetime = LocalDateTime.parse(str, formatter)
    }else{
      localdatetime = LocalDate.parse(str, formatter).atStartOfDay()
    }
    localdatetime.toInstant(zoneOffset).toEpochMilli
  }

  def toDate(str:String,pattern:String): LocalDateTime ={
    LocalDateTime.ofEpochSecond(toTs(str,pattern) / 1000L ,0,zoneOffset)
  }

  /**
   *
   * @param ts like 1616730764657
   * @return
   */
  def toDate(ts:Long): LocalDateTime ={
    LocalDateTime.ofEpochSecond(ts / 1000L,0,zoneOffset)
  }

}
