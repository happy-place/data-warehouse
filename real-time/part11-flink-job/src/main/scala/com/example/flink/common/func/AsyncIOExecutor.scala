package com.example.flink.common.func

import com.example.flink.util.{ThreadPoolUtil}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}

import java.util.concurrent.ThreadPoolExecutor

class AsyncIOExecutor[I,O](task:AsyncTask[I,O]) extends RichAsyncFunction[I,O] {

  private var pool:ThreadPoolExecutor = null

  override def open(parameters: Configuration): Unit = {
    pool = ThreadPoolUtil.getInstance()
  }

  override def asyncInvoke(input: I, resultFuture: ResultFuture[O]): Unit = {
    pool.submit(task.createRunnable(input,resultFuture))
  }

  override def close(): Unit = {
    if(pool!=null){
      pool.shutdown()
    }
  }

}
