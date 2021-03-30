package com.example.log.test

import com.example.log.util.MyKafkaUtil.send
import org.junit.Test

import java.text.SimpleDateFormat
import java.util.Date

class MyKafkaUtilTest {
  @Test
  def sendTest(): Unit ={
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = new Date()
    val future = send("test", sdf.format(date))
    if(!future.isDone){
      println(future.get().hasOffset)
    }
  }
}
