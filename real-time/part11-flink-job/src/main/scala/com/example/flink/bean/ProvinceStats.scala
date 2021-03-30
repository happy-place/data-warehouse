package com.example.flink.bean

import java.math.BigDecimal

case class ProvinceStats(
                          var stt: String = null,
                          var edt: String = null,
                          var province_id: Long = 0L,
                          var province_name: String = null,
                          var area_code: String = null,
                          var iso_code: String = null,
                          var iso_3166_2: String = null,
                          var order_amount: BigDecimal = BigDecimal.ZERO,
                          var order_count: Long = 0L,
                          var ts: Long = 0L
                        )
