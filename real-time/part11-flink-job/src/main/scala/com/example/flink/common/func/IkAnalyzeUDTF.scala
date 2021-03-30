package com.example.flink.common.func

import com.example.flink.util.KeywordUtil
import org.apache.flink.table.annotation.{DataTypeHint, FunctionHint}
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

@FunctionHint(output = new DataTypeHint("ROW<word STRING>"))
class IkAnalyzeUDTF extends TableFunction[Row]{

  def eval(text: String): Unit = {
    val words = KeywordUtil.analyze(text)
    words.foreach {s =>
      collect(Row.of(s))
//      val row = new Row(1)
//      row.setField(0,s)
//      collect(row)
    }
  }

}
