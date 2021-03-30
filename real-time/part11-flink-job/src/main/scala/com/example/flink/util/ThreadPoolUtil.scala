package com.example.flink.util

import java.util.concurrent._

/**
 * 懒汉单例线程池
 */
object ThreadPoolUtil {

  private var pool: ThreadPoolExecutor = null

  def getInstance(): ThreadPoolExecutor = {
    if (pool == null) {
      // 如果是实例就使用 this.synchronized
      ThreadPoolUtil.synchronized {
        if (pool == null) {
          init()
        }
      }
    }
    pool
  }

  private def init(): Unit = {
    val props = PropertiesUtil.load("config.properties")
    val corePoolSize = props.getProperty("aio.thread.pool.core-size").toInt
    val maxSize = props.getProperty("aio.thread.pool.max-size").toInt
    val ttlMins = props.getProperty("aio.thread.pool.ttl-minutes").toLong
    val alwaysCreate = props.getProperty("aio.thread.pool.always-create").toBoolean

    var blockingQueue: BlockingQueue[Runnable] = null
    if (alwaysCreate) {
      blockingQueue = new LinkedBlockingQueue[Runnable](Int.MaxValue)
    } else {
      blockingQueue = new ArrayBlockingQueue[Runnable](maxSize)
    }

    pool = new ThreadPoolExecutor(
      corePoolSize,
      maxSize,
      ttlMins,
      TimeUnit.MINUTES,
      blockingQueue
    )
  }

}
