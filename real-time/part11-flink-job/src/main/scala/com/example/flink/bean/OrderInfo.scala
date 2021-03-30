package com.example.flink.bean

import java.math.BigDecimal
import scala.beans.BeanProperty

/**
 * 订单
 * @param id
 * @param province_id
 * @param order_status
 * @param user_id
 * @param total_amount
 * @param activity_reduce_amount
 * @param coupon_reduce_amount
 * @param original_total_amount
 * @param feight_fee
 * @param expire_time
 * @param create_time
 * @param operate_time
 * @param create_date
 * @param create_hour
 * @param create_ts
 */
case class OrderInfo(
          @BeanProperty var id: Long,
          @BeanProperty var province_id: Long,
          @BeanProperty var order_status: String,
          @BeanProperty var user_id: Long,
          @BeanProperty var total_amount: BigDecimal,
          @BeanProperty var activity_reduce_amount: BigDecimal,
          @BeanProperty var coupon_reduce_amount: BigDecimal,
          @BeanProperty var original_total_amount: BigDecimal,
          @BeanProperty var feight_fee: BigDecimal,
          @BeanProperty var expire_time: String,
          @BeanProperty var create_time: String,
          @BeanProperty var operate_time: String,
          @BeanProperty var create_date: String,  // 依据 create_time 衍生
          @BeanProperty var create_hour: String, // 依据 create_time 衍生
          @BeanProperty var create_ts: Long  // 依据 create_time 衍生
)
