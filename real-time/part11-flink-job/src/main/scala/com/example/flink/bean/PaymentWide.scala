package com.example.flink.bean

/**
 * 订单支付宽表
 */

import org.apache.commons.beanutils.BeanUtils

import java.lang.reflect.InvocationTargetException
import java.math.BigDecimal
//import java.lang.{Long, Integer => Int}
import scala.beans.BeanProperty

/**
 * java 的反射
 * @param payment_id
 * @param subject
 * @param payment_type
 * @param payment_create_time
 * @param callback_time
 * @param detail_id
 * @param order_id
 * @param sku_id
 * @param order_price
 * @param sku_num
 * @param sku_name
 * @param province_id
 * @param order_status
 * @param user_id
 * @param total_amount
 * @param activity_reduce_amount
 * @param coupon_reduce_amount
 * @param original_total_amount
 * @param feight_fee
 * @param split_feight_fee
 * @param split_activity_amount
 * @param split_coupon_amount
 * @param split_total_amount
 * @param order_create_time
 * @param province_name
 * @param province_area_code
 * @param province_iso_code
 * @param province_3166_2_code
 * @param user_age
 * @param user_gender
 * @param spu_id
 * @param tm_id
 * @param category3_id
 * @param spu_name
 * @param tm_name
 * @param category3_name
 */

case class PaymentWide(
                     @BeanProperty var  payment_id:Long=0L,
                     @BeanProperty var  subject:String=null,
                     @BeanProperty var payment_type:String=null,
                     @BeanProperty var payment_create_time:String=null,
                     @BeanProperty var callback_time:String=null,
                     @BeanProperty var detail_id:Long=0L,
                     @BeanProperty var order_id:Long=0L,
                     @BeanProperty var sku_id:Long=0L,
                     @BeanProperty var order_price:BigDecimal=null,
                     @BeanProperty var sku_num:Long=0L,
                     @BeanProperty var sku_name:String=null,
                     @BeanProperty var province_id:Long=0L,
                     @BeanProperty var order_status:String=null,
                     @BeanProperty var user_id:Long=0L,
                     @BeanProperty var total_amount:BigDecimal=null,
                     @BeanProperty var activity_reduce_amount:BigDecimal=null,
                     @BeanProperty var coupon_reduce_amount:BigDecimal=null,
                     @BeanProperty var original_total_amount:BigDecimal=null,
                     @BeanProperty var feight_fee:BigDecimal=null,
                     @BeanProperty var split_feight_fee:BigDecimal=null,
                     @BeanProperty var split_activity_amount:BigDecimal=null,
                     @BeanProperty var split_coupon_amount:BigDecimal=null,
                     @BeanProperty var split_total_amount:BigDecimal=null,
                     @BeanProperty var order_create_time:String=null,
                     @BeanProperty var province_name:String=null, //查询维表得到
                     @BeanProperty var province_area_code:String=null,
                     @BeanProperty var province_iso_code:String=null,
                     @BeanProperty var province_3166_2_code:String=null,
                     @BeanProperty var user_age:Int=0,
                     @BeanProperty var user_gender:String=null,
                     @BeanProperty var spu_id:Long=0L,  //作为维度数据 要关联进来
                     @BeanProperty var tm_id:Long=0L,
                     @BeanProperty var category3_id:Long=0L,
                     @BeanProperty var spu_name:String=null,
                     @BeanProperty var tm_name:String=null,
                     @BeanProperty var category3_name:String=null
){

  def mergeOrderWide(orderWide: OrderWide):Unit = {
    if (orderWide != null) {
      try {
        BeanUtils.copyProperties(this, orderWide)
        this.order_create_time = orderWide.create_time
      } catch {
        case e:IllegalAccessException =>
          e.printStackTrace()
        case e:InvocationTargetException =>
          e.printStackTrace()
      }
    }
  }

  def mergePaymentInfo(paymentInfo: PaymentInfo):Unit = {
    if (paymentInfo != null) {
      try {
        BeanUtils.copyProperties(this,paymentInfo);
        payment_create_time=paymentInfo.create_time;
        this.payment_id = paymentInfo.id;
      } catch {
        case e:IllegalAccessException =>
          e.printStackTrace()
        case e:InvocationTargetException =>
          e.printStackTrace()
      }
    }
  }

  def this(paymentInfo:PaymentInfo, orderWide:OrderWide) = {
    this()
    mergeOrderWide(orderWide);
    mergePaymentInfo(paymentInfo)
  }

}
