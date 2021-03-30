package com.example.flink.bean

import java.lang.{Long => BigInt}

case class TestBean(
                     var spu_name: String,
                     var click_ct: BigInt,
                     var cart_ct: BigInt,
                     var order_ct: BigInt,
                     var stt: String,
                     var edt: String
                   )
