package com.example.flink.common.func

import org.apache.flink.streaming.api.scala.async.ResultFuture

abstract class AsyncTask[I,O] extends Serializable {

  def createRunnable(input: I, resultFuture: ResultFuture[O]): Runnable

}
