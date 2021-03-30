package com.example.flink.common.func

import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.fastjson.serializer.{SerializeConfig, SerializerFeature}
import com.example.flink.util.DimUtil
import org.apache.flink.streaming.api.scala.async.ResultFuture
import org.slf4j.LoggerFactory
import scala.reflect.ClassTag

/**
 * 维度关联抽象工具类
 * 为避免 Array(input) 中泛型被擦除，需要指定T为ClassTag类型
 * @param table
 * @param classTag$T$0
 * @tparam T
 */
abstract class DimJoinTask[T:ClassTag](table:String) extends AsyncTask[T,T]{

  private val log = LoggerFactory.getLogger(this.getClass)

  def getKey(t:T): Any

  def join(t:T,dimResult:JSONObject):Unit

  override def createRunnable(input: T, resultFuture: ResultFuture[T]): Runnable = {
    new Runnable() {
      override def run(): Unit = {
        val obj = DimUtil.queryOneWithCache(table, classOf[JSONObject], ("ID",getKey(input)))
        if(obj!=null && obj.size()>0){
          join(input,obj)
        }else{
          // TODO user binlog 同步延迟，可能造成维度关联失败，这里可能需要触发报警 WriteMapNullValue
          log.warn(s"${table}维度关联失败：${JSON.toJSONString(input,new SerializeConfig(true),
            SerializerFeature.WriteMapNullValue)} ${obj}")
        }
        // 哪怕未关联上也要输出，否则会报超时异常
        resultFuture.complete(Array(input))
      }
    }
  }

}
