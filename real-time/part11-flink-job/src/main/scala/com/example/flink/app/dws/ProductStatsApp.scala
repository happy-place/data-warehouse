package com.example.flink.app.dws

import com.alibaba.fastjson.serializer.{SerializeConfig, SerializerFeature}
import com.alibaba.fastjson.{JSON, JSONObject}
import com.example.flink.bean.{OrderWide, PaymentWide, ProductStats}
import com.example.flink.common.constant.GmallConstant
import com.example.flink.common.func.{AsyncIOExecutor, DimJoinTask}
import com.example.flink.util.{ClickhouseUtil, DateTimeUtil, KafkaUtil}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration
import java.util.concurrent.TimeUnit


/**
 * DWS 商品主题统计
 * 商品维度 点击、曝光、收藏、加入购物车、下单、支付、退款、评价 等指标统计
 *
 * dwd_page_log  点击、曝光  -> ProductStats()
 * dwd_favor_info 收藏 -> ProductStats()
 * dwd_cart_info 加入购物车 -> ProductStats()
 * dwm_order_wide 下单 -> ProductStats()
 * dwm_payment_wide 支付 -> ProductStats()
 * dwd_order_refund_info 退款 -> ProductStats()
 * dwd_comment_info 评价 -> ProductStats()
 *
 *  -> 合并统计对象 union(ProductStats)
 *  -> 指定时间字段、乱序水印
 *  -> 分组 keyBy(sku_id)
 *  -> 开窗 TumblingTimeWindow
 *  -> 聚合 reduce(ReduceFunc 累计,WindowProcessFunc 添加窗口起止时间标记)
 *  -> AsyncFunc 补充sku、spu、category3、trademark 维度
 *  -> 输出到 clickhouse
 *
 */

object ProductStatsApp {

  def main(args:Array[String]):Unit = {
    // todo 1 初始化环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    env.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.setStateBackend(new FsStateBackend("hdfs://hadoop01:9000/flink/checkpoint/gmall/product_stats_app"))
    env.setRestartStrategy(RestartStrategies.noRestart())

    val dimQueryTimeout = 30
    val groupId = "product_stats_app_group"

    // todo 2 创建kafka流，读取dwd dwm 层数据
    val pageTopic = "dwd_page_log"
    val favorTopic = "dwd_favor_info"
    val cartTopic = "dwd_cart_info"
    val orderTopic = "dwm_order_wide"
    val paymentTopic = "dwm_payment_wide"
    val refundTopic = "dwd_order_refund_info"
    val commentTopic = "dwd_comment_info"

    val sinkTopic = "dws_product_stats"

    val pageDS = env.addSource(KafkaUtil.createKafkaSource(pageTopic,groupId)).map(JSON.parseObject(_))
    val favorDS = env.addSource(KafkaUtil.createKafkaSource(favorTopic,groupId)).map(JSON.parseObject(_))
    val cartDS = env.addSource(KafkaUtil.createKafkaSource(cartTopic,groupId)).map(JSON.parseObject(_))
    val orderDS = env.addSource(KafkaUtil.createKafkaSource(orderTopic,groupId)).map(JSON.parseObject(_,classOf[OrderWide]))
    val paymentDS = env.addSource(KafkaUtil.createKafkaSource(paymentTopic,groupId)).map(JSON.parseObject(_,classOf[PaymentWide]))
    val refundDS = env.addSource(KafkaUtil.createKafkaSource(refundTopic,groupId)).map(JSON.parseObject(_))
    val commentDS = env.addSource(KafkaUtil.createKafkaSource(commentTopic,groupId)).map(JSON.parseObject(_))


    // todo 3 封装 ProductStats 统计对象，对各自统计指标打点
    // todo 3.1 商品点击、曝光 打点 (process = filter + map)
    val clickDisplayStatsDS = pageDS.process{
      new ProcessFunction[JSONObject,ProductStats]() {
        override def processElement(json: JSONObject, context: ProcessFunction[JSONObject, ProductStats]#Context,
                                    collector: Collector[ProductStats]): Unit = {
                val ts = json.getLong("ts")
                val page = json.getJSONObject("page")
                val pageId = page.getString("page_id")

                if("good_detail".equals( pageId)){
                  val sku_id = page.getLong("item")
                  val stats = ProductStats.apply(ts = ts, sku_id = sku_id, click_ct = 1L)
                  collector.collect(stats)
                }

                val displays = json.getJSONArray("displays")
                if(displays!=null && displays.size()>0){
                  displays.forEach{any =>
                    val display = any.asInstanceOf[JSONObject]
                    if("sku_id".equals(display.getString("item_type"))){
                      val sku_id = display.getLong("item")
                      val stats = ProductStats.apply(ts=ts,sku_id=sku_id,display_ct=1L)
                      collector.collect(stats)
                    }
                  }
                }
        }
      }
    }

    // todo 3.2 收藏 打点
    val favorStatsDS = favorDS.process {
      new ProcessFunction[JSONObject,ProductStats]() {
        override def processElement(json: JSONObject, context: ProcessFunction[JSONObject, ProductStats]#Context,
                                    collector: Collector[ProductStats]): Unit = {
          val sku_id = json.getLong("sku_id")
          val isCancel = json.getString("is_cancel").equals("1")
          val ts = DateTimeUtil.toTs(json.getString("create_time"),"yyyy-MM-dd HH:mm:ss")
          if(!isCancel){
            val stats = ProductStats.apply(ts=ts,sku_id=sku_id,favor_ct = 1L)
            collector.collect(stats)
          }

        }
      }
    }

    // todo 3.3 加入购物车 打点
    val cartStatsDS = cartDS.map{json =>
      val sku_id = json.getLong("sku_id")
      val ts = DateTimeUtil.toTs(json.getString("create_time"),"yyyy-MM-dd HH:mm:ss")
      val stats = ProductStats.apply(ts=ts,sku_id=sku_id,cart_ct=1L)
      stats
    }

    // todo 3.4 下单 打点
    val orderStatsDS = orderDS.map{ orderWide =>
      val stats = ProductStats.apply(ts=orderWide.create_ts,
        sku_id=orderWide.sku_id,
        order_sku_num=orderWide.sku_num,
        order_amount=orderWide.split_total_amount
      )
      stats.order_id_set.add(orderWide.order_id)
      stats
    }

    // todo 3.5 支付 打点
    val paymentStatsDS = paymentDS.map{paymentWide=>
      val ts = DateTimeUtil.toTs(paymentWide.order_create_time,"yyyy-MM-dd HH:mm:ss")
      val stats = ProductStats.apply(ts=ts,
        sku_id=paymentWide.sku_id,
        payment_amount=paymentWide.split_total_amount)
      stats.paid_order_id_set.add(paymentWide.order_id)
      stats
    }

    // todo 3.6 退款 打卡
    val refundStatsDS = refundDS.map{json =>
      val sku_id = json.getLong("sku_id")
      val ts = DateTimeUtil.toTs(json.getString("create_time"),"yyyy-MM-dd HH:mm:ss")
      val refund_amount = json.getBigDecimal("refund_amount")
      val order_id = json.getLong("order_id")
      val stats = ProductStats.apply(ts=ts,sku_id=sku_id,refund_amount=refund_amount)
      stats.refund_order_id_set.add(order_id)
      stats
    }

    // todo 3.7 评价 打点
    val commentStatsDS = commentDS.map{json =>
      val sku_id = json.getLong("sku_id")
      val ts = DateTimeUtil.toTs(json.getString("create_time"),"yyyy-MM-dd HH:mm:ss")
      val is_good = json.getString("appraise").equals(GmallConstant.APPRAISE_GOOD)
      val stats = ProductStats.apply(ts=ts,sku_id=sku_id,comment_ct=1L)
      if(is_good){
        stats.good_comment_ct = 1L
      }
      stats
    }

    // todo 4 合并统计对象
    val unionDS = clickDisplayStatsDS.union(favorStatsDS,cartStatsDS,orderStatsDS,paymentStatsDS,refundStatsDS,commentStatsDS)

    // todo 5 指定时间字段、乱序水印
    val unionWithWatermarkDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))
      .withTimestampAssigner(
      new SerializableTimestampAssigner[ProductStats](){
        override def extractTimestamp(element: ProductStats, recordTimestamp: Long): Long = element.ts
      }
    ))

    // todo 6 分组
    val keyedDS = unionWithWatermarkDS.keyBy(_.sku_id)

    // todo 7 开窗
//    val windowDS = keyedDS.timeWindow(Time.seconds(10))
    val windowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)))

    // todo 8 聚合
    val produceStatsDS = windowDS.reduce(
      new ReduceFunction[ProductStats]() { // 统计
        override def reduce(value1: ProductStats, value2: ProductStats): ProductStats = {
          value1.click_ct = value1.click_ct + value2.click_ct
          value1.display_ct = value1.display_ct + value2.display_ct
          value1.favor_ct = value1.favor_ct + value2.favor_ct
          value1.cart_ct = value1.cart_ct + value2.cart_ct
          value1.order_sku_num = value1.order_sku_num + value2.order_sku_num
          value1.order_amount = value1.order_amount + value2.order_amount
          value2.order_id_set.foreach(value1.order_id_set.add(_))
          value1.order_ct = value1.order_id_set.size
          value1.payment_amount = value1.payment_amount + value2.payment_amount
          value2.paid_order_id_set.foreach(value1.paid_order_id_set.add(_))
          value1.paid_order_ct = value1.paid_order_id_set.size
          value1.refund_amount = value1.refund_amount + value2.refund_amount
          value2.refund_order_id_set.foreach(value1.refund_order_id_set.add(_))
          value1.refund_order_ct = value1.refund_order_id_set.size
          value1.comment_ct = value1.comment_ct + value2.comment_ct
          value1.good_comment_ct = value1.good_comment_ct + value2.good_comment_ct
          value1
        }
      },
      new ProcessWindowFunction[ProductStats,ProductStats,Long,TimeWindow]() { // 带上窗口标识 （起止时间）
        override def process(key: Long, context: Context, elements: Iterable[ProductStats],
                             out: Collector[ProductStats]): Unit = {
          val pattern = "yyyy-MM-dd HH:mm:ss"
          val windowStart = DateTimeUtil.toStr(context.window.getStart,pattern)
          val windowEnd = DateTimeUtil.toStr(context.window.getEnd,pattern)
          elements.foreach{stats =>
            stats.stt = windowStart
            stats.edt = windowEnd
            out.collect(stats)
          }
        }
      }
    )

    // todo 9 异步查询补充商品维度
    // todo 9.1 query DIM_SKU_INFO 补充SKU_NAME，SPU_ID，CATEGORY3_ID，TM_ID
    val statsWithSkuDS = AsyncDataStream.unorderedWait(produceStatsDS,
      new AsyncIOExecutor[ProductStats,ProductStats](new DimJoinTask[ProductStats]("DIM_SKU_INFO"){
        override def getKey(t: ProductStats): Any = t.sku_id
        override def join(t: ProductStats, dimResult: JSONObject): Unit = {
          t.sku_name = dimResult.getString("SKU_NAME")
          t.sku_price = dimResult.getBigDecimal("PRICE")
          t.category3_id = dimResult.getLong("CATEGORY3_ID")
          t.tm_id = dimResult.getLong("TM_ID")
          t.spu_id = dimResult.getLong("SPU_ID")
        }
      }),
      dimQueryTimeout,
      TimeUnit.SECONDS
    )

    // todo 9.2 query DIM_SPU_INFO 补充 SPU_NAME
    val statsWithSkuSpuDS = AsyncDataStream.unorderedWait(statsWithSkuDS,
      new AsyncIOExecutor[ProductStats,ProductStats](new DimJoinTask[ProductStats]("DIM_SPU_INFO"){
        override def getKey(t: ProductStats): Any = t.spu_id
        override def join(t: ProductStats, dimResult: JSONObject): Unit = {
          t.spu_name = dimResult.getString("SPU_NAME")
        }
      }),
      dimQueryTimeout,
      TimeUnit.SECONDS
    )

    // todo 9.3 query DIM_BASE_CATEGORY3 补充 CATEGORY3_NAME
    val statsWithSkuSpuCate3DS = AsyncDataStream.unorderedWait(statsWithSkuSpuDS,
      new AsyncIOExecutor[ProductStats,ProductStats](new DimJoinTask[ProductStats]("DIM_BASE_CATEGORY3"){
        override def getKey(t: ProductStats): Any = t.category3_id
        override def join(t: ProductStats, dimResult: JSONObject): Unit = {
          t.category3_name = dimResult.getString("NAME")
        }
      }),
      dimQueryTimeout,
      TimeUnit.SECONDS
    )

    // todo 9.4 query DIM_BASE_TRADEMARK 补充 TM_NAME
    val statsWithSkuSpuCate3TmDS = AsyncDataStream.unorderedWait(statsWithSkuSpuCate3DS,
      new AsyncIOExecutor[ProductStats,ProductStats](new DimJoinTask[ProductStats]("DIM_BASE_TRADEMARK"){
        override def getKey(t: ProductStats): Any = t.tm_id
        override def join(t: ProductStats, dimResult: JSONObject): Unit = {
          t.tm_name = dimResult.getString("TM_NAME")
        }
      }),
      dimQueryTimeout,
      TimeUnit.SECONDS
    )

    // todo 10 输出到clickhouse（olap） 和 kafka（product keyword analysis）
//    statsWithSkuSpuCate3TmDS.map{stats =>
//      JSON.toJSONString(stats,new SerializeConfig(true),SerializerFeature.WriteMapNullValue)
//    }.print()

    val sql = s"insert into dws_product_stats values(${("?" * 25).mkString(",")})"
    statsWithSkuSpuCate3TmDS.addSink(ClickhouseUtil.createSink[ProductStats](sql))

    statsWithSkuSpuCate3TmDS.map(JSON.toJSONString(_,new SerializeConfig(true)))
      .addSink(KafkaUtil.createKafkaSink(sinkTopic))

    // todo 11 启动执行
    env.execute("ProductStatsApp Job")
  }

}
