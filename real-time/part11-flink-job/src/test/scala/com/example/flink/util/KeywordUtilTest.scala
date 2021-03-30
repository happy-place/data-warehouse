package com.example.flink.util

import org.junit.Test

class KeywordUtilTest {


  /**
   * ext_dict.txt 中存在的，不在继续分词
   * ext_dict.txt 和 ext_stopwords.txt 都存在的，停止词生效
   */
  @Test
  def test1(): Unit ={
    val strings = KeywordUtil.analyze("Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待")
    println(strings.mkString(","))
  }

  @Test
  def test2(): Unit ={
    val strings = KeywordUtil.analyze("Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待",false)
    println(strings.mkString(","))
  }

}
