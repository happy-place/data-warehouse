package com.example.flink.bean

import scala.beans.BeanProperty

/**
 * binlog 被收集到一个topic后需要借助flink进行分流，实时表写入kafka dwd层，维度表写入hbase
 * 通过TableProcess 映射被一条binlog的分流方式
 * 由于使用反射映射对象，因此必须添加 @BeanProperty 注解，才能生成getter、setter，且必须使用空参构造器，反射才能创建实例
 * @param sourceTable
 * @param operateType
 * @param sinkType
 * @param sinkTable
 * @param sinkColumns
 * @param sinkPk
 * @param sinkExtend
 */
case class TableProcess(
       @BeanProperty var sourceTable:String = null,
       @BeanProperty var operateType:String = null,
       @BeanProperty var sinkType:String = null,
       @BeanProperty var sinkTable:String = null,
       @BeanProperty var sinkColumns:String = null,
       @BeanProperty var sinkPk:String = null,
       @BeanProperty var sinkExtend:String = null
) {
  def this()= this(null)
}
