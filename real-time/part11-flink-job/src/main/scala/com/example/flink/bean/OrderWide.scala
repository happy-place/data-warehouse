package com.example.flink.bean

import org.apache.commons.lang3.ObjectUtils

import java.math.BigDecimal
//import java.lang.{Long, Integer => Int}
import scala.beans.BeanProperty

case class OrderWide(@BeanProperty var detail_id: Long = 0L,
                     @BeanProperty var order_id: Long = 0L,
                     @BeanProperty var sku_id: Long= 0L, // 关联sku_id 同时，带出 spu_id、tm_id、catetory3_id
                     @BeanProperty var order_price: BigDecimal= null,
                     @BeanProperty var sku_num: Long= 0L,
                     @BeanProperty var sku_name: String= null,
                     @BeanProperty var province_id: Long= 0L,
                     @BeanProperty var order_status: String= null,
                     @BeanProperty var user_id: Long= 0L,

                     @BeanProperty var total_amount: BigDecimal= null,
                     @BeanProperty var activity_reduce_amount: BigDecimal= null,
                     @BeanProperty var coupon_reduce_amount: BigDecimal= null,
                     @BeanProperty var original_total_amount: BigDecimal= null,
                     @BeanProperty var feight_fee: BigDecimal= null,
                     @BeanProperty var split_feight_fee: BigDecimal= null,
                     @BeanProperty var split_activity_amount: BigDecimal= null,
                     @BeanProperty var split_coupon_amount: BigDecimal= null,
                     @BeanProperty var split_total_amount: BigDecimal= null,

                     @BeanProperty var expire_time: String= null,
                     @BeanProperty var create_time: String= null,
                     @BeanProperty var operate_time: String= null,
                     @BeanProperty var create_date: String= null, // 把其他字段处理得到
                     @BeanProperty var create_hour: String= null,
                     @BeanProperty var create_ts: Long= 0L,

                     @BeanProperty var province_name: String= null, //查询维表得到
                     @BeanProperty var province_area_code: String= null,
                     @BeanProperty var province_iso_code: String= null,
                     @BeanProperty var province_3166_2_code: String= null,

                     @BeanProperty var user_age: Int= 0,
                     @BeanProperty var user_gender: String= null,

                     @BeanProperty var spu_id: Long= 0L, //作为维度数据 要关联进来
                     @BeanProperty var tm_id: Long= 0L,
                     @BeanProperty var category3_id: Long= 0L,
                     @BeanProperty var spu_name: String= null,
                     @BeanProperty var tm_name: String= null,
                     @BeanProperty var category3_name: String = null
                    ) {

  def this(orderInfo:OrderInfo, orderDetail: OrderDetail) = {
    this()
    mergeOrderInfo(orderInfo)
    mergeOrderDetail(orderDetail)
  }

  def mergeOrderInfo(orderInfo: OrderInfo){
    if (orderInfo != null) {
      this.order_id = orderInfo.id
      this.order_status = orderInfo.order_status
      this.create_time = orderInfo.create_time
      this.create_date = orderInfo.create_date
      this.create_hour = orderInfo.create_hour
      this.create_ts = orderInfo.create_ts
      this.activity_reduce_amount = orderInfo.activity_reduce_amount
      this.coupon_reduce_amount = orderInfo.coupon_reduce_amount
      this.original_total_amount = orderInfo.original_total_amount
      this.feight_fee = orderInfo.feight_fee
      this.total_amount =  orderInfo.total_amount
      this.province_id = orderInfo.province_id
      this.user_id = orderInfo.user_id
    }
  }

  def mergeOrderDetail(orderDetail: OrderDetail) {
    if (orderDetail != null) {
      this.detail_id = orderDetail.id
      this.sku_id = orderDetail.sku_id
      this.sku_name = orderDetail.sku_name
      this.order_price = orderDetail.order_price
      this.sku_num = orderDetail.sku_num
      this.split_activity_amount=orderDetail.split_activity_amount
      this.split_coupon_amount=orderDetail.split_coupon_amount
      this.split_total_amount=orderDetail.split_total_amount
    }
  }

  def mergeOtherOrderWide(otherOrderWide:OrderWide){
    this.order_status = ObjectUtils.firstNonNull( this.order_status ,otherOrderWide.order_status)
    this.create_time =  ObjectUtils.firstNonNull(this.create_time,otherOrderWide.create_time)
    this.create_date =  ObjectUtils.firstNonNull(this.create_date,otherOrderWide.create_date)
    this.create_hour =  ObjectUtils.firstNonNull(this.create_hour,otherOrderWide.create_hour)
    this.create_ts =  ObjectUtils.firstNonNull(this.create_ts,otherOrderWide.create_ts)
    this.coupon_reduce_amount =  ObjectUtils.firstNonNull(this.coupon_reduce_amount,otherOrderWide.coupon_reduce_amount)
    this.activity_reduce_amount =  ObjectUtils.firstNonNull(this.activity_reduce_amount,otherOrderWide.activity_reduce_amount)
    this.original_total_amount =  ObjectUtils.firstNonNull(this.original_total_amount,otherOrderWide.original_total_amount)
    this.feight_fee = ObjectUtils.firstNonNull( this.feight_fee,otherOrderWide.feight_fee)
    this.total_amount =  ObjectUtils.firstNonNull( this.total_amount,otherOrderWide.total_amount)
    this.user_id =  ObjectUtils.firstNonNull(this.user_id,otherOrderWide.user_id)
    this.sku_id = ObjectUtils.firstNonNull( this.sku_id,otherOrderWide.sku_id)
    this.sku_name =  ObjectUtils.firstNonNull(this.sku_name,otherOrderWide.sku_name)
    this.order_price =  ObjectUtils.firstNonNull(this.order_price,otherOrderWide.order_price)
    this.sku_num = ObjectUtils.firstNonNull( this.sku_num,otherOrderWide.sku_num)
    this.split_activity_amount=ObjectUtils.firstNonNull(this.split_activity_amount)
    this.split_coupon_amount=ObjectUtils.firstNonNull(this.split_coupon_amount)
    this.split_total_amount=ObjectUtils.firstNonNull(this.split_total_amount)
  }

}
