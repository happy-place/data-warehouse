package com.example.log.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.config.HttpClientConfig.Builder
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Cat, CatResult, Index}

import scala.collection.JavaConversions._

object MyESUtil {
  private var factory: JestClientFactory = null
  private val urls = MyPropertiesUtil.load("config.properties").getProperty("elastic.urls").split(",").toList
  private val config: HttpClientConfig = new Builder(urls)
                                          .multiThreaded(true)
                                          .maxTotalConnection(20)
                                          .connTimeout(10000).readTimeout(10000).build()

  def getClient: JestClient = {
    // 单例
    if (factory == null) build()
    factory.setHttpClientConfig(config)
    factory.getObject
  }

  private def build(): Unit = {
    factory = new JestClientFactory
  }

  def bulkInsert(bulkMap: Map[String, Any]): BulkResult = {
    // key 为id:index,value为样例类实例
    val client = getClient
    val bulkBuilder = new Bulk.Builder()
    bulkMap.foreach(entry => {
      val arr = entry._1.split(":")
      val id = arr(0)
      val index = arr(1)
      bulkBuilder.addAction(new Index.Builder(entry._2).index(index).`type`("_doc").id(id).build())
    })
    val bulkReq = bulkBuilder.build()
    val result: BulkResult = client.execute(bulkReq)
    client.close()
    result
  }

  def main(args: Array[String]): Unit = {
    val catReq = new Cat.NodesBuilder().build()

    //获取操作对象
    val jest: JestClient = getClient

    //执行操作
    val result: CatResult = jest.execute(catReq)

    // Array[Array[String]]
    val tabs = result.getPlainText

    //获取Hit中的source部分
    tabs.foreach(rows => println(rows.mkString("\t")))
    //关闭连接
    jest.close()
  }

}
