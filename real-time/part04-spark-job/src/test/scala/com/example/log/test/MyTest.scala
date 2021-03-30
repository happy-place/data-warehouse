package com.example.log.test

import org.junit.Test

class MyTest {

  @Test
  def sort():Unit = {
    val list = List(2,3,-1,5)
    // 升序排序
    val ints = list.sortWith((o1, o2) => o1 < o2)
    println(ints.mkString(","))
  }

  @Test
  def test1(): Unit ={
    for(i <- 0 to 3){ // [0,3]
      println(i)
    }
    for(i <- 0 until 3){ // [0,3)
      println(i)
    }
  }

  @Test
  def test2():Unit = {
    println(Some(1).get)
    println(Option.empty.get)
  }

}
