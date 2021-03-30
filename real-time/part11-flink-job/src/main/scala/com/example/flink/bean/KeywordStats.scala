package com.example.flink.bean

/**
 * 搜索关键词统计
 *
 * @param keyword
 * @param ct
 * @param source
 * @param stt
 * @param edt
 * @param ts
 */
case class KeywordStats(
                         var stt: String = null,
                         var edt: String = null,
                         var keyword: String = null,
                         var source: String = null,
                         var ct: Long = 0L,
                         var ts: Long = 0L
                       )
