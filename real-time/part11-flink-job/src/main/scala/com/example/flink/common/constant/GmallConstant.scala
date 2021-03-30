package com.example.flink.common.constant

/**
 * 电商相关常量
 */
object GmallConstant {

  //10 单据状态
  val ORDER_STATUS_UNPAID: String = "1001" //未支付

  val ORDER_STATUS_PAID: String = "1002" //已支付

  val ORDER_STATUS_CANCEL: String = "1003" //已取消

  val ORDER_STATUS_FINISH: String = "1004" //已完成

  val ORDER_STATUS_REFUND: String = "1005" //退款中

  val ORDER_STATUS_REFUND_DONE: String = "1006" //退款完成


  //11 支付状态
  val PAYMENT_TYPE_ALIPAY: String = "1101" //支付宝

  val PAYMENT_TYPE_WECHAT: String = "1102" //微信

  val PAYMENT_TYPE_UNION: String = "1103" //银联


  //12 评价
  val APPRAISE_GOOD: String = "1201" // 好评

  val APPRAISE_SOSO: String = "1202" // 中评

  val APPRAISE_BAD: String = "1203" //  差评

  val APPRAISE_AUTO: String = "1204" // 自动


  //13 退货原因
  val REFUND_REASON_BAD_GOODS: String = "1301" // 质量问题

  val REFUND_REASON_WRONG_DESC: String = "1302" // 商品描述与实际描述不一致

  val REFUND_REASON_SALE_OUT: String = "1303" //   缺货

  val REFUND_REASON_SIZE_ISSUE: String = "1304" //  号码不合适

  val REFUND_REASON_MISTAKE: String = "1305" //  拍错

  val REFUND_REASON_NO_REASON: String = "1306" //  不想买了

  val REFUND_REASON_OTHER: String = "1307" //    其他


  //14 购物券状态
  val COUPON_STATUS_UNUSED: String = "1401" //    未使用

  val COUPON_STATUS_USING: String = "1402" //     使用中

  val COUPON_STATUS_USED: String = "1403" //       已使用


  //15退款类型
  val REFUND_TYPE_ONLY_MONEY: String = "1501" //   仅退款

  val REFUND_TYPE_WITH_GOODS: String = "1502" //    退货退款


  //24来源类型
  val SOURCE_TYPE_QUREY: String = "2401" //   用户查询

  val SOURCE_TYPE_PROMOTION: String = "2402" //   商品推广

  val SOURCE_TYPE_AUTO_RECOMMEND: String = "2403" //   智能推荐

  val SOURCE_TYPE_ACTIVITY: String = "2404" //   促销活动


  //购物券范围
  val COUPON_RANGE_TYPE_CATEGORY3: String = "3301" //

  val COUPON_RANGE_TYPE_TRADEMARK: String = "3302"
  val COUPON_RANGE_TYPE_SPU: String = "3303"

  //购物券类型
  val COUPON_TYPE_MJ: String = "3201" //满减

  val COUPON_TYPE_DZ: String = "3202" // 满量打折

  val COUPON_TYPE_DJ: String = "3203" //  代金券


  val ACTIVITY_RULE_TYPE_MJ: String = "3101"
  val ACTIVITY_RULE_TYPE_DZ: String = "3102"
  val ACTIVITY_RULE_TYPE_ZK: String = "3103"


  val KEYWORD_SEARCH: String = "SEARCH"
  val KEYWORD_CLICK: String = "CLICK"
  val KEYWORD_CART: String = "CART"
  val KEYWORD_ORDER: String = "ORDER"

}
