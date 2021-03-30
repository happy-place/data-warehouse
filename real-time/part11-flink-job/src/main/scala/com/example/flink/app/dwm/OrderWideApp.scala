package com.example.flink.app.dwm

import com.alibaba.fastjson.serializer.{SerializeConfig, SerializerFeature}
import com.alibaba.fastjson.{JSON, JSONObject}
import com.example.flink.common.func.{AsyncIOExecutor, DimJoinTask}
import com.example.flink.bean.{OrderDetail, OrderInfo, OrderWide}
import com.example.flink.util.{DateTimeUtil, KafkaUtil}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.time.{Duration, LocalDate}
import java.util.concurrent.TimeUnit

/**
 * 执行流程：
 * kafka -> dwd_order_info interval_join dwd_order_detail -> order_wide_stream
 *
 * async_func (order_wide_stream add dim like prov、user_info、sku_info、spu_info、base_category3、trademark_info)
 *
 * sink -> dwm_order_wide
 *
 * 前期准备：
 * mysql maxwell hadoop phoenix redis kafka db_dist
 *
 */
object OrderWideApp {

  def main(args:Array[String]):Unit = {
    // todo 1 初始化环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    env.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(300000)
    env.setStateBackend(new FsStateBackend("hdfs://hadoop01:9000/flink/checkpoint/gmall/order_wide_app"))
    env.setRestartStrategy(RestartStrategies.noRestart())

    val groupId = "order_wide_app_group"
    val orderInfoTopic = "dwd_order_info"
    val orderDetailTopic = "dwd_order_detail"
    val orderWideTopic = "dwm_order_wide"

    // todo 2 获取订单、订单明细流
    val orderInfoSource= KafkaUtil.createKafkaSource(orderInfoTopic, groupId)
    val orderInfoKafkaStream = env.addSource(orderInfoSource)
    val orderInfoStream = orderInfoKafkaStream.map{
      new RichMapFunction[String,OrderInfo](){
        var sdf:SimpleDateFormat = null

        override def open(parameters: Configuration): Unit = {
          sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        }

        override def map(value: String): OrderInfo = {
          val orderInfo = JSON.parseObject(value,classOf[OrderInfo])
          val createTime = orderInfo.create_time
          val arr = createTime.split(" ")
          orderInfo.create_date = arr(0)
          orderInfo.create_hour = arr(1).split(":")(0)
          orderInfo.create_ts = sdf.parse(createTime).getTime
          orderInfo
        }
      }
    }

    val orderDetailSource = KafkaUtil.createKafkaSource(orderDetailTopic, groupId)
    val orderDetailKafkaStream = env.addSource(orderDetailSource)
    val orderDetailStream = orderDetailKafkaStream.map(
      new RichMapFunction[String,OrderDetail](){
        var sdf:SimpleDateFormat = null

        override def open(parameters: Configuration): Unit = {
          sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        }

        override def map(value: String): OrderDetail = {
          val orderDetail = JSON.parseObject(value,classOf[OrderDetail])
          orderDetail.create_ts = sdf.parse(orderDetail.create_time).getTime
          orderDetail
        }
      }
    )

//    orderInfoStream.print("order_info")
//    orderDetailStream.print("order_detail")

    // todo 3 指定双流各自的时间标识、乱序水印，为双流拼接做准备
    val orderInfoWithTsDS = orderInfoStream.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(
        new SerializableTimestampAssigner[OrderInfo](){
          override def extractTimestamp(element: OrderInfo, recordTimestamp: Long): Long = element.create_ts
        })
    )

    val orderDetailWithTsDS = orderDetailStream.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(
        new SerializableTimestampAssigner[OrderDetail]() {
          override def extractTimestamp(element: OrderDetail, recordTimestamp: Long): Long = element.create_ts
        }
      ))

    // todo 4 声明双流的键
    val orderInfoKeyedByMidWithTsDS = orderInfoWithTsDS.keyBy(_.id)
    val orderDetailKeyedByMidWithTsDS = orderDetailWithTsDS.keyBy(_.order_id)


    /**
     * todo 5 双流使用interval join拼接
     * 3.双流join 生成 orderWide流
     * interval Join: 相当于inner join,不支持left join，无重复数据
     */
    val orderWideDS = orderInfoKeyedByMidWithTsDS.intervalJoin(orderDetailKeyedByMidWithTsDS)
      .between(Time.seconds(-5),Time.seconds(5))
      .process(
        new ProcessJoinFunction[OrderInfo,OrderDetail,OrderWide](){
          override def processElement(info: OrderInfo, detail: OrderDetail,
                                      context: ProcessJoinFunction[OrderInfo, OrderDetail, OrderWide]#Context,
                                      collector: Collector[OrderWide]): Unit = {
            collector.collect(new OrderWide(info, detail))
          }
        }
      )

//  orderWideDS.print("order_wide")

    // todo 5 异步查询，关联维度
    // todo 5.1 join DIM_USER_INFO 补全 user_age, user_gender
    val timeoutSeconds = 300
    val wideWithUserDimDS = AsyncDataStream.unorderedWait(
      orderWideDS,
      new AsyncIOExecutor[OrderWide, OrderWide](new DimJoinTask[OrderWide]("DIM_USER_INFO"){
        override def getKey(t: OrderWide): Any = t.user_id
        override def join(t: OrderWide, dimResult: JSONObject): Unit = {
          val pattern = "yyyy-MM-dd"
          val birthday = DateTimeUtil.toDate(dimResult.getString("BIRTHDAY"), pattern).getYear
          val now = LocalDate.now().getYear
          val age = now - birthday
          t.user_age = age
          val gender = dimResult.getString("GENDER")
          t.user_gender = gender
        }
      }),
      timeoutSeconds, TimeUnit.SECONDS
    )

    // todo 5.2 join DIM_BASE_PROVINCE 补全 province_name，province_area_code，province_iso_code，province_3166_2_code
    val wideWithUserProvDimDS = AsyncDataStream.unorderedWait(
      wideWithUserDimDS,
      new AsyncIOExecutor[OrderWide, OrderWide](new DimJoinTask[OrderWide]("DIM_BASE_PROVINCE"){
        override def getKey(t: OrderWide): Any = t.province_id
        override def join(t: OrderWide, dimResult: JSONObject): Unit = {
          t.province_3166_2_code = dimResult.getString("ISO_3166_2")
          t.province_area_code = dimResult.getString("AREA_CODE")
          t.province_iso_code = dimResult.getString("ISO_CODE")
          t.province_name = dimResult.getString("NAME")
        }
      }),
      timeoutSeconds, TimeUnit.SECONDS
    )

    // todo 5.3 join DIM_SKU_INFO 补全 sku_name,spu_id,tm_id,category3_id, 注意：sku_num 已经包含在 dwd_order_detail 消息中
    val wideWithUserProvSkuDimDS = AsyncDataStream.unorderedWait(
      wideWithUserProvDimDS,
      new AsyncIOExecutor[OrderWide, OrderWide](new DimJoinTask[OrderWide]("DIM_SKU_INFO"){
        override def getKey(t: OrderWide): Any = t.sku_id
        override def join(t: OrderWide, dimResult: JSONObject): Unit = {
          t.sku_name = dimResult.getString("SKU_NAME")
          t.tm_id = dimResult.getString("TM_ID").toLong
          t.category3_id = dimResult.getString("CATEGORY3_ID").toLong
          t.spu_id = dimResult.getString("SPU_ID").toLong
        }
      }),
      timeoutSeconds, TimeUnit.SECONDS
    )

    // todo 5.4 join DIM_SPU_INFO 补全 spu_name
    val wideWithUserProvSkuSpuDimDS = AsyncDataStream.unorderedWait(
      wideWithUserProvSkuDimDS,
      new AsyncIOExecutor[OrderWide, OrderWide](new DimJoinTask[OrderWide]("DIM_SPU_INFO"){
        override def getKey(t: OrderWide): Any = t.spu_id
        override def join(t: OrderWide, dimResult: JSONObject): Unit = {
          t.spu_name = dimResult.getString("SPU_NAME")
        }
      }),
      timeoutSeconds, TimeUnit.SECONDS
    )

    // todo 5.5 join DIM_BASE_CATEGORY3 补全 category3_name
    val wideWithUserProvSkuSpuCategory3DimDS = AsyncDataStream.unorderedWait(
      wideWithUserProvSkuSpuDimDS,
      new AsyncIOExecutor[OrderWide, OrderWide](new DimJoinTask[OrderWide]("DIM_BASE_CATEGORY3"){
        override def getKey(t: OrderWide): Any = t.category3_id
        override def join(t: OrderWide, dimResult: JSONObject): Unit = {
          t.category3_name = dimResult.getString("NAME")
        }
      }),
      timeoutSeconds, TimeUnit.SECONDS
    )

    // todo 5.6 join DIM_BASE_TRADEMARK 补全 tm_name
    val wideWithUserProvSkuSpuCategory3TrademarkDimDS = AsyncDataStream.unorderedWait(
      wideWithUserProvSkuSpuCategory3DimDS,
      new AsyncIOExecutor[OrderWide, OrderWide](new DimJoinTask[OrderWide]("DIM_BASE_TRADEMARK"){
        override def getKey(t: OrderWide): Any = t.tm_id
        override def join(t: OrderWide, dimResult: JSONObject): Unit = {
          t.tm_name = dimResult.getString("TM_NAME")
        }
      }),
      timeoutSeconds, TimeUnit.SECONDS
    )

//    wideWithUserProvSkuSpuCategory3TrademarkDimDS.map{wide =>
//      JSON.toJSONString(wide,new SerializeConfig(true), SerializerFeature.WriteMapNullValue)
//    }.print("order_wide")

    // todo 6 结果输出到 kafka dwm_order_wide
    val orderWideSink = KafkaUtil.createKafkaSink(orderWideTopic)
    wideWithUserProvSkuSpuCategory3TrademarkDimDS.map(
      JSON.toJSONString(_,new SerializeConfig(true), SerializerFeature.WriteMapNullValue)
    ).addSink(orderWideSink)

    // todo 7 启动计算
    env.execute("OrderWideApp Job")
  }

}
