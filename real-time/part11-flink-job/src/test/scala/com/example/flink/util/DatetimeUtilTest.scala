package com.example.flink.util

import org.junit.Test

import java.time.LocalDateTime
import java.util.Date

class DatetimeUtilTest {

  @Test
  def toDateTest(): Unit ={
    val date = new Date()
    println(date)
    val date1 = DateTimeUtil.toDate(date.getTime)
    println(date1)

    val date2 = DateTimeUtil.toDate("2021-03-26 11:50:40", "yyyy-MM-dd HH:mm:ss")
    println(date2)
  }

  @Test
  def toTsTest(): Unit ={
    val date = new Date()
    println(date.getTime)
    val ts1 = DateTimeUtil.toTs(date)
    println(ts1)

    val ts2 = DateTimeUtil.toTs("2021-03-26 11:50:40", "yyyy-MM-dd HH:mm:ss")
    println(ts2)

    val ts3 = DateTimeUtil.toTs("2021-03-26 11", "yyyy-MM-dd HH")
    println(ts3)

    val ts4 = DateTimeUtil.toTs("2021-03-26", "yyyy-MM-dd")
    println(ts4)
  }

  @Test
  def toStrTest(): Unit ={
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val date = new Date()

    println(date)

    val str1 = DateTimeUtil.toStr(date,pattern)
    println(str1)

    val str2 = DateTimeUtil.toStr(1616730640000L,pattern)
    println(str2)
  }

  @Test
  def test(): Unit ={
    println(DateTimeUtil.toDate("1979-03-21","yyyy-MM-dd"))
    println(LocalDateTime.now().getYear)
  }

}
