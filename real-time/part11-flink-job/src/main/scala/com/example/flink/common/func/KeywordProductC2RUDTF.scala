package com.example.flink.common.func

import com.example.flink.common.constant.GmallConstant
import org.apache.flink.table.annotation.{DataTypeHint, FunctionHint}
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row
import java.lang.{Long => BIGINT}

@FunctionHint(output = new DataTypeHint("ROW<ct BIGINT,source STRING>"))
class KeywordProductC2RUDTF extends TableFunction[Row]{

  /**
   * LATERAL TABLE(keywordProductC2R(click_ct ,cart_ct,order_ct)) AS T2(ct,source)
   * @param text
   */
  def eval(click_ct: BIGINT,cart_ct: BIGINT,order_ct: BIGINT): Unit = {
    collect(Row.of(click_ct,GmallConstant.KEYWORD_CLICK))
    collect(Row.of(cart_ct,GmallConstant.KEYWORD_CART))
    collect(Row.of(order_ct,GmallConstant.KEYWORD_ORDER))
   /* if(click_ct>0L){
//      val row = new Row(2)
//      row.setField(0,click_ct)
//      row.setField(1,GmallConstant.KEYWORD_CLICK)
      collect(Row.of(click_ct,GmallConstant.KEYWORD_CLICK))
    }

    if(cart_ct>0L){
//      val row = new Row(2)
//      row.setField(0,cart_ct)
//      row.setField(1,GmallConstant.KEYWORD_CART)
      collect(Row.of(cart_ct,GmallConstant.KEYWORD_CART))
    }

    if(order_ct>0L){
//      val row = new Row(2)
//      row.setField(0,order_ct)
//      row.setField(1,GmallConstant.KEYWORD_ORDER)
      collect(Row.of(order_ct,GmallConstant.KEYWORD_ORDER))
    }*/

  }

}
