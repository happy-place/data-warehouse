package com.example.flink.bean

import java.math.BigDecimal
//import java.lang.{Long, Integer => Int}
import scala.beans.BeanProperty

case class PaymentInfo(
       @BeanProperty id:Long,
       @BeanProperty order_id:Long,
       @BeanProperty user_id:Long,
       @BeanProperty total_amount:BigDecimal,
       @BeanProperty subject:String,
       @BeanProperty payment_type:String,
       @BeanProperty create_time:String,
       @BeanProperty var create_ts:Long,
       @BeanProperty callback_time:String
)
