package com.example.flink.bean

import com.example.flink.common.annotation.TransientSink
//import java.lang.{Long, Integer => Int}
import scala.collection.mutable.HashSet

/**
 * 商品统计
 *
 * @param stt
 * @param edt
 * @param sku_id
 * @param sku_name
 * @param sku_price
 * @param spu_id
 * @param spu_name
 * @param tm_id
 * @param tm_name
 * @param category3_id
 * @param category3_name
 * @param display_ct
 * @param click_ct
 * @param favor_ct
 * @param cart_ct
 * @param order_sku_num
 * @param order_amount
 * @param order_ct
 * @param payment_amount
 * @param paid_order_ct
 * @param refund_order_ct
 * @param refund_amount
 * @param comment_ct
 * @param good_comment_ct
 * @param order_id_set
 * @param paid_order_id_set
 * @param refund_order_id_set
 * @param ts
 */
case class ProductStats(
                         var stt: String = null, //窗口起始时间
                         var edt: String = null, //窗口结束时间
                         var sku_id: Long = 0L, //sku编号
                         var sku_name: String = null, //sku名称
                         var sku_price: BigDecimal = BigDecimal.apply(0), //sku单价
                         var spu_id: Long = 0L, //spu编号
                         var spu_name: String = null, //spu名称
                         var tm_id: Long = 0L, //品牌编号
                         var tm_name: String = null, //品牌名称
                         var category3_id: Long = 0L, //品类编号
                         var category3_name: String = null, //品类名称
                         var display_ct: Long = 0L, //曝光数
                         var click_ct: Long = 0L, //点击数
                         var favor_ct: Long = 0L, //收藏数
                         var cart_ct: Long = 0L, //添加购物车数
                         var order_sku_num: Long = 0L, //下单商品个数
                         var order_amount: BigDecimal = BigDecimal.apply(0), //下单商品金额
                         var order_ct: Long = 0L, //订单数
                         var payment_amount: BigDecimal = BigDecimal.apply(0), //支付金额
                         var paid_order_ct: Long = 0L, //支付订单数
                         var refund_order_ct: Long = 0L, //退款订单数
                         var refund_amount: BigDecimal = BigDecimal.apply(0),
                         var comment_ct: Long = 0L, //评论订单数
                         var good_comment_ct: Long = 0L, //好评订单数
                         var ts: Long = 0L //统计时间戳
                       ) {
  @TransientSink
  var order_id_set: HashSet[Long] = new HashSet[Long] //用于统计订单数
  @TransientSink
  var paid_order_id_set: HashSet[Long] = new HashSet[Long] //用于统计支付订单数
  @TransientSink
  var refund_order_id_set: HashSet[Long] = new HashSet[Long] //用于退款支付订单数
}
