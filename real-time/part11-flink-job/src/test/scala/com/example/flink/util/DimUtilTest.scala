package com.example.flink.util

import com.alibaba.fastjson.JSONObject
import org.junit.Test

class DimUtilTest {

  @Test
  def test1():Unit = {
    val nObject = DimUtil.queryOneWithCache("dim_base_category1", classOf[JSONObject], ("id", "11"))
    println(nObject)
  }

  @Test
  def test2():Unit = {
    val nObject = DimUtil.queryOneWithNoCache("dim_base_category1", classOf[JSONObject], ("id", "11"))
    println(nObject)
  }


}
