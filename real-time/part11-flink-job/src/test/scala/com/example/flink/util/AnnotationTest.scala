package com.example.flink.util

import com.example.flink.bean.ProductStats
import com.example.flink.common.annotation.{CustomAnnotation, CustomAnnotationUsage, TransientSink}
import org.junit.Test

import scala.reflect.runtime.universe._
import reflect.runtime.universe._
import scala.annotation.StaticAnnotation

class AnnotationTest {

  // 获取指定类型的注解信息，通过 Annotation.tree.tpe 获取注解的 Type 类型，以此进行筛选
  def getClassAnnotation[T: TypeTag, U: TypeTag] =
    symbolOf[T].annotations.find(_.tree.tpe =:= typeOf[U])

  // 通过字段名称获取指定类型的注解信息，注意查找字段名称时添加空格
  def getMemberAnnotation[T: TypeTag, U: TypeTag](memberName: String) =
    typeOf[T].decl(TermName(s"$memberName ")).annotations.find(_.tree.tpe =:= typeOf[U])

  // 通过方法名称和参数名称获取指定类型的注解信息
  def getArgAnnotation[T: TypeTag, U: TypeTag](methodName: String, argName: String) =
    typeOf[T].decl(TermName(methodName)).asMethod.paramLists.collect {
      case symbols => symbols.find(_.name == TermName(argName))
    }.headOption.fold(Option[Annotation](null))(_.get.annotations.find(_.tree.tpe =:= typeOf[U]))

  // 解析语法树，获取注解数据
  def getCustomAnnotationData(tree: Tree) = {
    val Apply(_, Literal(Constant(name: String)) :: Literal(Constant(num: Int)) :: Nil) = tree
    new CustomAnnotation(name, num)
  }

  def listProperties1[T:TypeTag,U:TypeTag](): List[(TermSymbol, Annotation)] = {
    // a field is a Term that is a Var or a Val
    val fields = typeOf[T].members.collect{ case s: TermSymbol => s }.
      filter(s => s.isVal || s.isVar)

    // then only keep the ones with a MyProperty annotation
    fields.flatMap(f => f.annotations.find(_.tpe =:= typeOf[U]).
      map((f, _))).toList
  }

  def listProperties2[T:TypeTag,U:TypeTag](T:Class[T],U:Class[U]): List[(TermSymbol, Annotation)] = {
    // a field is a Term that is a Var or a Val
    val fields = typeOf[T].members.collect{ case s: TermSymbol => s }.
      filter(s => s.isVal || s.isVar)

    // then only keep the ones with a MyProperty annotation
    fields.flatMap(f => f.annotations.find(_.tpe =:= typeOf[U]).
      map((f, _))).toList
  }

  @Test
  def test1(): Unit ={
    println(listProperties1[CustomAnnotationUsage,CustomAnnotation])
  }

  @Test
  def test2(): Unit ={
    val tuples = listProperties1[ProductStats, TransientSink]
    tuples.foreach(t => println(t._1.fullName))
    println(listProperties1[ProductStats,TransientSink])
  }

  @Test
  def test3(): Unit ={
    val fields = AnnotationUtil.getAnnotationFieldNames[ProductStats,TransientSink]
    println(fields.mkString(","))
  }

  @Test
  def test4(): Unit ={
    println(listProperties2(classOf[ProductStats],classOf[TransientSink]))
  }

  @Test
  def classAnnoTest(): Unit = {
    getClassAnnotation[CustomAnnotationUsage, CustomAnnotation].map(_.tree) foreach { classAnnotationTree =>
      val classAnnotation = getCustomAnnotationData(classAnnotationTree)
      println(classAnnotation)
    }
  }

  @Test
  def memberAnnoTest(): Unit = {
    getMemberAnnotation[CustomAnnotationUsage, CustomAnnotation]("ff").map(_.tree) foreach { memberAnnotationTree =>
      val memberAnnotation = getCustomAnnotationData(memberAnnotationTree)
      println(memberAnnotation)
    }
  }

  @Test
  def argsAnnoTest(): Unit = {
    getArgAnnotation[CustomAnnotationUsage, CustomAnnotation]("mm", "arg").map(_.tree) foreach { argAnnotationTree =>
      val argAnnotation = getCustomAnnotationData(argAnnotationTree)
      println(argAnnotation)
    }
  }

}