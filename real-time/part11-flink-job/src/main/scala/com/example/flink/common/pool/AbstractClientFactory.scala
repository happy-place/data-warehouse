package com.example.flink.common.pool

import org.apache.commons.pool.BasePoolableObjectFactory

/**
 * 创建连接相关对象的抽象工厂，注意是创建连接对象的工厂，不是池子的工厂
 * @tparam T
 */
abstract class AbstractClientFactory[T <: AutoCloseable] extends BasePoolableObjectFactory {

  def makeObject(): AnyRef

  // TODO BasePoolableObjectFactory 中已经有默认实现，推荐自己实现
  // def validateObject(o: Any): Boolean

  override def destroyObject(o: Any): Unit = {
    if(o.isInstanceOf[T]){
      o.asInstanceOf[T].close()
    }
  }

}