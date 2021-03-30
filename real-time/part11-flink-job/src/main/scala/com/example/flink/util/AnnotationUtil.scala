package com.example.flink.util

import reflect.runtime.universe._

object AnnotationUtil {

  def getAnnotationFields[Usage:TypeTag,Anno:TypeTag](): List[(TermSymbol, Annotation)] = {
    // a field is a Term that is a Var or a Val
    val fields = typeOf[Usage].members.collect{ case s: TermSymbol => s }.
      filter(s => s.isVal || s.isVar)

    // then only keep the ones with a MyProperty annotation
    fields.flatMap(f => f.annotations.find(_.tpe =:= typeOf[Anno]).
      map((f, _))).toList
  }

  def getAnnotationFieldNames[Usage:TypeTag,Anno:TypeTag](): List[String] = {
    getAnnotationFields[Usage,Anno].map(_._1.fullName.split("\\.").last)
  }

}
