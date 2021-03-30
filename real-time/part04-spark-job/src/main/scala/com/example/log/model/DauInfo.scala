package com.example.log.model

/**
 * DAU，事实表（从每天启动日志中摘取）,存储在ES
 * @param mid
 * @param uid
 * @param ar
 * @param ch
 * @param vc
 * @param dt
 * @param hr
 * @param mi
 * @param ts
 */
case class DauInfo(
                    mid: String, //设备id
                    uid: String, //用户id
                    ar: String, //地区
                    ch: String, //渠道
                    vc: String, //版本
                    var dt: String, //日期
                    var hr: String, //小时
                    var mi: String, //分钟
                    ts: Long //时间戳
                  )
