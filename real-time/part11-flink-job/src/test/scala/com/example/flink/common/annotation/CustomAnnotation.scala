package com.example.flink.common.annotation

import scala.annotation.StaticAnnotation

class CustomAnnotation(name: String, num: Int) extends StaticAnnotation {
  override def toString = s"Annotation args: name -> $name, num -> $num"
}