package com.example.log.test

import com.example.log.util.MyESUtil.getClient
import io.searchbox.client.JestClient
import io.searchbox.core.{Bulk, Index, Search, SearchResult}
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, TermQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder
import org.elasticsearch.search.sort.SortOrder
import org.junit.Test

import java.{util => jutil}

case class Movie(id: String, movie_name: String, actorNameList: jutil.List[String]) {}

class ESPutTest {

  @Test
  def putIndex(): Unit = {
    //建立连接
    val jest: JestClient = getClient
    //Builder中的参数，底层会转换为Json格式字符串，所以我们这里封装Document为样例类
    //当然也可以直接传递json
    val actorNameList = new jutil.ArrayList[String]()
    actorNameList.add("zhangsan")
    val index: Index = new Index.Builder(Movie("100", "red", actorNameList))
      .index("movie_index")
      .`type`("movie")
      .id("1")
      .build()
    //execute的参数类型为Action，Action是接口类型，不同的操作有不同的实现类，添加的实现类为Index
    jest.execute(index)
    //关闭连接
    jest.close()
  }

  @Test
  def bulkPut(): Unit = {
    //建立连接
    val jest: JestClient = getClient
    //Builder中的参数，底层会转换为Json格式字符串，所以我们这里封装Document为样例类
    //当然也可以直接传递json
    val actorNameList = new jutil.ArrayList[String]()
    actorNameList.add("zhangsan")

    val index: Index = new Index.Builder(Movie("100", "red", actorNameList))
      .index("movie_index")
      .`type`("movie") // es 6.x 必须带type，默认type 是 _doc
      .id("1")
      .build()

    val bulkReq = new Bulk.Builder().addAction(index).build()

    //execute的参数类型为Action，Action是接口类型，不同的操作有不同的实现类，添加的实现类为Index
    val result = jest.execute(bulkReq)
    println(result.isSucceeded)
    //关闭连接
    jest.close()
  }

  @Test
  def getIndex0(): Unit = {
    //查询常用有两个实现类 Get通过id获取单个Document，以及Search处理复杂查询
    val query =
      """
        |{
        |  "query": {
        |    "bool": {
        |       "must": [
        |        {"match": {
        |          "name": "river"
        |        }}
        |      ]
        |    }
        |  }
        |}
    """.stripMargin
    val search: Search = new Search.Builder(query)
      .addIndex("movie_index")
      .build()

    //获取操作对象
    val jest: JestClient = getClient

    //执行操作
    val result: SearchResult = jest.execute(search)
    //获取命中的结果  sourceType:对命中的数据进行封装，因为是Json，所以我们用map封装
    //注意：一定得是Java的Map类型
    val rsList: jutil.List[SearchResult#Hit[jutil.Map[String, Any], Void]] = result.getHits(classOf[jutil.Map[String, Any]])

    //将Java转换为Scala集合，方便操作
    import scala.collection.JavaConverters._
    //获取Hit中的source部分
    val list: List[jutil.Map[String, Any]] = rsList.asScala.map(_.source).toList
    println(list.mkString("\n"))
    //关闭连接
    jest.close()
  }

  @Test
  def getIndex(): Unit = {
    //查询常用有两个实现类 Get通过id获取单个Document，以及Search处理复杂查询
    val query =
      """
        |{
        |  "query": {
        |    "bool": {
        |       "must": [
        |        {"match": {
        |          "movie_name": "red"
        |        }}
        |      ],
        |      "filter": [
        |        {"term": { "actorList.name.keyword": "zhang cuishan"}}
        |      ]
        |    }
        |  },
        |  "from": 0,
        |  "size": 20,
        |  "sort": [
        |    {
        |      "doubanScore": {
        |        "order": "desc"
        |      }
        |    }
        |  ],
        |  "highlight": {
        |    "fields": {
        |      "name": {}
        |    }
        |  }
        |}
    """.stripMargin
    val search: Search = new Search.Builder(query)
      .addIndex("movie_index")
      .build()

    //获取操作对象
    val jest: JestClient = getClient

    //执行操作
    val result: SearchResult = jest.execute(search)
    //获取命中的结果  sourceType:对命中的数据进行封装，因为是Json，所以我们用map封装
    //注意：一定得是Java的Map类型
    val rsList: jutil.List[SearchResult#Hit[jutil.Map[String, Any], Void]] = result.getHits(classOf[jutil.Map[String, Any]])

    //将Java转换为Scala集合，方便操作
    import scala.collection.JavaConverters._
    //获取Hit中的source部分
    val list: List[jutil.Map[String, Any]] = rsList.asScala.map(_.source).toList
    println(list.mkString("\n"))
    //关闭连接
    jest.close()
  }

  @Test
  def getIndex2(): Unit = {
    //通过SearchSourceBuilder构建查询语句
    val sourceBuilder: SearchSourceBuilder = new SearchSourceBuilder
    val boolQueryBuilder = new BoolQueryBuilder
    boolQueryBuilder.must(new MatchQueryBuilder("name", "red"))
    boolQueryBuilder.filter(new TermQueryBuilder("actorList.name.keyword", "zhang cuishan"))
    sourceBuilder.query(boolQueryBuilder)
    sourceBuilder.from(0)
    sourceBuilder.size(20)
    sourceBuilder.sort("doubanScore", SortOrder.DESC)
    sourceBuilder.highlighter(new HighlightBuilder().field("name"))
    val query2 = sourceBuilder.toString

    val search: Search = new Search.Builder(query2)
      .addIndex("movie_index")
      .build()

    //获取操作对象
    val jest: JestClient = getClient

    //执行操作
    val result: SearchResult = jest.execute(search)
    //获取命中的结果  sourceType:对命中的数据进行封装，因为是Json，所以我们用map封装
    //注意：一定得是Java的Map类型
    val rsList: jutil.List[SearchResult#Hit[jutil.Map[String, Any], Void]] = result.getHits(classOf[jutil.Map[String, Any]])

    //将Java转换为Scala集合，方便操作
    import scala.collection.JavaConverters._
    //获取Hit中的source部分
    val list: List[jutil.Map[String, Any]] = rsList.asScala.map(_.source).toList
    println(list.mkString("\n"))
    //关闭连接
    jest.close()

  }

}
