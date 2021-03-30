package com.example.flink.bean

import scala.beans.BeanProperty

/**
 * 访问统计
 */
case class VisitorStats(
                         @BeanProperty var stt: String = null,
                         //统计结束时间
                         @BeanProperty var edt: String = null,
                         //维度：版本
                         @BeanProperty var vc: String = null,
                         //维度：渠道
                         @BeanProperty var ch: String = null,
                         //维度：地区
                         @BeanProperty var ar: String = null,
                         //维度：新老用户标识
                         @BeanProperty var is_new: String = null,
                         //度量：独立访客数
                         @BeanProperty var uv_ct: Long = 0L,
                         //度量：页面访问数
                         @BeanProperty var pv_ct: Long = 0L,
                         //度量： 进入次数
                         @BeanProperty var sv_ct: Long = 0L,
                         //度量： 跳出次数
                         @BeanProperty var uj_ct: Long = 0L,
                         //度量： 持续访问时间
                         @BeanProperty var dur_sum: Long = 0L,
                         //统计时间
                         @BeanProperty var ts: Long = 0L
                       )
