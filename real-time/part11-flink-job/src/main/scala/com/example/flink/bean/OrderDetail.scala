package com.example.flink.bean

import java.math.BigDecimal
import scala.beans.BeanProperty



/**
 * 订单明细
 *
 * JSON.parse(text,clazz) 中 scala 的 BigDecimal 会报错，因此使用java的.
 * @BeanProperty 注解方便java反射时设置对象属性
 *
 * @param id
 * @param order_id
 * @param sku_id
 * @param order_price
 * @param sku_num
 * @param sku_name
 * @param create_time
 * @param split_total_amount
 * @param split_activity_amount
 * @param split_coupon_amount
 * @param create_ts
 */
case class OrderDetail (
 @BeanProperty var id:Long,
 @BeanProperty var order_id:Long,
 @BeanProperty var sku_id:Long,
 @BeanProperty var order_price: BigDecimal,
 @BeanProperty var sku_num:Long,
 @BeanProperty var sku_name: String,
 @BeanProperty var create_time: String,
 @BeanProperty var split_total_amount: BigDecimal,
 @BeanProperty var split_activity_amount: BigDecimal,
 @BeanProperty var split_coupon_amount: BigDecimal,
 @BeanProperty var create_ts: Long, // 基于create_time 衍生
)