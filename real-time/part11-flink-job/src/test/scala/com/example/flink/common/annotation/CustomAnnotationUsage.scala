package com.example.flink.common.annotation

//@CustomAnnotation("Annotation for Class", 2333)
class CustomAnnotationUsage {
  @CustomAnnotation("Annotation for Member", 6666)
  val ff = ""

  def mm(ss: String, @CustomAnnotation("Annotation for Arg", 9999) arg: Int) = ""
}