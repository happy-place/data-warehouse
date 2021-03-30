package com.example.flink.app.dws

import com.alibaba.fastjson.serializer.{SerializeConfig, SerializerFeature}
import com.alibaba.fastjson.{JSON, JSONObject}
import com.example.flink.bean.VisitorStats
import com.example.flink.util.{ClickhouseUtil, DateTimeUtil, KafkaUtil}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.time.Duration
import java.util.Date

/**
 * DWS 访问主题统计
 * 地区、渠道、版本、新老用户维度 统计 页面访问次数 pv、独立访问数 uv、访问时长 dur、进入页面用户数 sv、跳出用户数 jv
 *
 * 功能：基于dwd_start_log 和 dwm_visit_jump 日志，从area、渠道、版本、新老用户维度统计pv、uv、sv、dur，jv
 * 执行流程：
 * applog -> nginx -> kafka 集中上报 (ods_base_log) -> BaseLogApp 分流出（dwd_page_log）-> VisitJumpApp 过滤出 (dwm_visit_jump)
 * -> UniqueVisitApp 过滤出 (dwm_unique_visit)
 *
 * dwd_page_log -> pvDurDS 独立访问次数
 * dwd_page_log -> filter -> svDS 进入页面次数，（进入首个页面才参与统计，即last_page_id不存在）
 * dwm_unique_visit -> uvDS 独立访问人数
 * dwm_visit_jump -> ujDS 跳出页面次数 （进入首个页面，15秒内没有跳转其他页面记录）
 *
 * pvDurDS.union(svDS,uvDS,ujDS) -> keyBy(ar,ch,vc,is_new)
 * -> assign watermark 指定event-time,watermark -> trumbling window 指定统计窗口
 * -> reduce(agg 合并 ,window-process添加时间标签) -> clickhouse
 *
 * 注：对哪条流进行开窗统计，就在哪条流上添加水印，指定时间
 */
object VisitorStatsApp {

  def main(args: Array[String]): Unit = {
    // todo 1 初始化环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.setStateBackend(new FsStateBackend("hdfs://hadoop01:9000/flink/checkpoint/gmall/visitor_stats_app"))
    env.setRestartStrategy(RestartStrategies.noRestart())

    val groupId = "visitor_stats_app_group"
    val pageTopic = "dwd_page_log"
    val jumpTopic = "dwm_visit_jump"
    val uniqueVisitTopic = "dwm_unique_visit"
    val clickhouseTable = "dws_visitor_stats"

    // todo 2 获取页面访问、访问跳出、独立访问流
    val pageSource = KafkaUtil.createKafkaSource(pageTopic, groupId)
    val jumpSource = KafkaUtil.createKafkaSource(jumpTopic, groupId)
    val uniqueVisitSource = KafkaUtil.createKafkaSource(uniqueVisitTopic, groupId)

    val pageDS = env.addSource(pageSource).map(JSON.parseObject(_))
    val jumpDS = env.addSource(jumpSource).map(JSON.parseObject(_))
    val uniqueVisitDS = env.addSource(uniqueVisitSource).map(JSON.parseObject(_))

    //    pageDS.print("page")
    //    jumpDS.print("jump")
    //    uniqueVisitDS.print("unique")

    // todo 3 对三种流分别封装VisitorStats对象，针对pv,uv,dur_sum,uj四种指标各自打点
    // todo 3.1 基于页面访问流，封装VisitorStats 对pv_ct和dur_sum进行打点
    val pvDurStatsDS = pageDS.map { json =>
      val common = json.getJSONObject("common")
      val page = json.getJSONObject("page")
      VisitorStats(
        "", // stt 统计开始时间
        "", // edt 统计结束时间
        common.getString("vc"), // 维度：版本
        common.getString("ch"), // 维度：渠道
        common.getString("ar"), // 维度：地区
        common.getString("is_new"), // 维度：新老用户
        0L, // 度量：独立访客数 uv_ct
        1L, //度量：页面访问数 pv_ct
        0L, //度量： 进入次数  sv_ct
        0L, //度量： 跳出次数 uj_ct
        page.getLong("during_time"), //度量： 持续访问时间 dur_sum
        common.getLong("ts") // 日志时间
      )
    }

    // todo 3.2 基于独立访问流，封装VisitorStats 对uv_ct进行打点
    val uvStatsDS = uniqueVisitDS.map { json =>
      val common = json.getJSONObject("common")
      VisitorStats(
        "", // stt 统计开始时间
        "", // edt 统计结束时间
        common.getString("vc"), // 维度：版本
        common.getString("ch"), // 维度：渠道
        common.getString("ar"), // 维度：地区
        common.getString("is_new"), // 维度：新老用户
        1L, // 度量：独立访客数 uv_ct
        0L, //度量：页面访问数 pv_ct
        0L, //度量： 进入次数  sv_ct
        0L, //度量： 跳出次数 uj_ct
        0L, //度量： 持续访问时间 dur_sum
        json.getLong("ts") // 日志时间
      )
    }

    // todo 3.3 基于页面访问流，封装VisitorStats 对sv_ct进行打点
    // 方案1：先filter 过滤出访问首个页面记录，然后封装对象
    //    val svStatsDS = pageDS.filter { json =>
    //      val lastPageId = json.getJSONObject("page").getString("last_page_id")
    //      lastPageId == null || lastPageId.size == 0
    //    }.map { json =>
    //      val common = json.getJSONObject("common")
    //      VisitorStats(
    //        "", // stt 统计开始时间
    //        "", // edt 统计结束时间
    //        common.getString("vc"), // 维度：版本
    //        common.getString("ch"), // 维度：渠道
    //        common.getString("ar"), // 维度：地区
    //        common.getString("is_new"), // 维度：新老用户
    //        0L, // 度量：独立访客数 uv_ct
    //        0L, //度量：页面访问数 pv_ct
    //        1L, //度量： 进入次数  sv_ct
    //        0L, //度量： 跳出次数 uj_ct
    //        0L, //度量： 持续访问时间 dur_sum
    //        json.getLong("ts") // 日志时间
    //      )
    //    }

    // 方案2:借助 process 可以一次性将 filter 和 map 都处理了
    val svStatsDS = pageDS.process(new ProcessFunction[JSONObject, VisitorStats]() {
      override def processElement(json: JSONObject, context: ProcessFunction[JSONObject, VisitorStats]#Context,
                                  collector: Collector[VisitorStats]): Unit = {
        val lastPageId = json.getJSONObject("page").getString("last_page_id")
        if(lastPageId == null || lastPageId.size == 0){
          val common = json.getJSONObject("common")
          val visitorStats = VisitorStats(
            "", // stt 统计开始时间
            "", // edt 统计结束时间
            common.getString("vc"), // 维度：版本
            common.getString("ch"), // 维度：渠道
            common.getString("ar"), // 维度：地区
            common.getString("is_new"), // 维度：新老用户
            0L, // 度量：独立访客数 uv_ct
            0L, //度量：页面访问数 pv_ct
            1L, //度量： 进入次数  sv_ct
            0L, //度量： 跳出次数 uj_ct
            0L, //度量： 持续访问时间 dur_sum
            json.getLong("ts") // 日志时间
          )
          collector.collect(visitorStats)
        }
      }
    })

    // todo 3.3 基于访问跳出流，封装VisitorStats 对uj_ct进行打点
    val ujStatsDS = jumpDS.map { json =>
      val common = json.getJSONObject("common")
      VisitorStats(
        "", // stt 统计开始时间
        "", // edt 统计结束时间
        common.getString("vc"), // 维度：版本
        common.getString("ch"), // 维度：渠道
        common.getString("ar"), // 维度：地区
        common.getString("is_new"), // 维度：新老用户
        0L, // 度量：独立访客数 uv_ct
        0L, //度量：页面访问数 pv_ct
        0L, //度量： 进入次数  sv_ct
        1L, //度量： 跳出次数 uj_ct
        0L, //度量： 持续访问时间 dur_sum
        json.getLong("ts") // 日志时间
      )
    }

    // todo 4 合并所有VisitorStats流
    val unionDS = pvDurStatsDS.union(uvStatsDS, svStatsDS, ujStatsDS)

    // todo 5 汇总统计

    // todo 5.1 指定时间语义和乱序水印
    // 窗口作用在哪条流上，就对哪条流添加watermark
    val visitorStatsWithWatermarkDS = unionDS.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[VisitorStats](Duration.ofSeconds(3)).withTimestampAssigner(
        new SerializableTimestampAssigner[VisitorStats] {
          override def extractTimestamp(element: VisitorStats, recordTimestamp: Long): Long = element.ts
        }
      ))

    // todo 5.2 分组，指定维度（地区、渠道、版本、新老用户）
    val keyedUnionDS = visitorStatsWithWatermarkDS.keyBy { visitStats =>
      (visitStats.ar, visitStats.ch, visitStats.vc, visitStats.is_new)
    }

    // todo 5.3 开窗 滚动窗口
    //        val windowDS = keyedUnionDS.timeWindow(Time.seconds(10))
    val windowDS = keyedUnionDS.window(TumblingEventTimeWindows.of(Time.seconds(10)))

    // todo 5.4 统计 （累计，补充窗口时间）
    val visitorStateDS = windowDS.reduce(
      new ReduceFunction[VisitorStats]() {
        override def reduce(value1: VisitorStats, value2: VisitorStats): VisitorStats = {
          value1.pv_ct = value1.pv_ct + value2.pv_ct
          value1.uv_ct = value1.uv_ct + value2.uv_ct // dwm_unique_visit 已经进行去重，可以直接累加
          value1.sv_ct = value1.sv_ct + value2.sv_ct
          value1.uj_ct = value1.uj_ct + value2.uj_ct // dwm_visit_jump 也已经去重，可以直接累加
          value1.dur_sum = value1.dur_sum + value2.dur_sum
          value1
        }
      },
      new ProcessWindowFunction[VisitorStats, VisitorStats, Tuple4[String, String, String, String], TimeWindow]() {
        override def process(key: (String, String, String, String), context: Context,
                             elements: Iterable[VisitorStats], out: Collector[VisitorStats]): Unit = {
          val pattern = "yyyy-MM-dd HH:mm:ss"
          val start = DateTimeUtil.toStr(context.window.getStart, pattern)
          val end = DateTimeUtil.toStr(context.window.getEnd, pattern)
          elements.foreach { visitorStats =>
            visitorStats.stt = start
            visitorStats.edt = end
            out.collect(visitorStats)
          }
        }
      }
    )

    visitorStateDS.map(JSON.toJSONString(_, new SerializeConfig(true), SerializerFeature.WriteMapNullValue))
      .print("visitor_stats")

    // todo 6 输出到 clickhouse
    val sql  = s"insert into ${clickhouseTable} values(${("?" * 12).mkString(",")})"
    visitorStateDS.addSink(ClickhouseUtil.createSink[VisitorStats](sql,10))

    // todo 7 启动计算
    env.execute("VisitorStatsApp Job")
  }


}
