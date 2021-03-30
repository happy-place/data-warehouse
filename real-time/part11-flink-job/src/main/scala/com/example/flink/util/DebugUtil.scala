package com.example.flink.util

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object DebugUtil {

  def interruptedException[T](ds: DataStream[T])(implicit evidence : org.apache.flink.api.common.typeinfo.TypeInformation[T]): Unit ={
    ds.process(new ProcessFunction[T,T](){
      override def processElement(i: T, context: ProcessFunction[T, T]#Context, collector: Collector[T]): Unit = {
        throw new RuntimeException("interrupt for debug")
      }
    })
  }

}
