package com.example.flink.util

import org.wltea.analyzer.core.IKSegmenter
import org.wltea.analyzer.dic.Dictionary

import java.io.StringReader
import java.util.Collections
import scala.collection.mutable.ListBuffer

/**
 * 分词辅助器
 */
object KeywordUtil {

  def analyze(text:String,useSmart:Boolean=true): Seq[String] ={
    val lexemes = ListBuffer[String]()
    val reader = new StringReader(text)
    val segmenter = new IKSegmenter(reader, useSmart)
    var loop = true
    while(loop){
      val lexeme = segmenter.next()
      if(lexeme!=null){
        lexemes.append(lexeme.getLexemeText)
      }else{
        loop = false
      }
    }
    lexemes
  }

}
