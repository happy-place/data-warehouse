package com.example.flink.common.annotation

import scala.annotation.{StaticAnnotation}

class TransientSink extends StaticAnnotation{
  override def toString = s"Annotation arg"
}
