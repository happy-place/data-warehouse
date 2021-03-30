package com.example.flink.util

import com.alibaba.fastjson.JSON
import com.example.flink.bean.{OrderInfo, OrderWide, PaymentInfo, PaymentWide, ProductStats}
import org.junit.Test

import java.text.SimpleDateFormat
import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.reflect.runtime.universe._

class CommonTest {

  @Test
  def test1(): Unit = {
    val json = "{\"common\":{\"ar\":\"530000\",\"uid\":\"18\",\"os\":\"Android 11.0\",\"ch\":\"web\",\"is_new\":\"1\",\"md\":\"Xiaomi 10 Pro \",\"mid\":\"mid_12\",\"vc\":\"v2.1.134\",\"ba\":\"Xiaomi\"},\"start\":{\"entry\":\"icon\",\"open_ad_skip_ms\":4017,\"open_ad_ms\":8290,\"loading_time\":4462,\"open_ad_id\":8},\"ts\":1616400091000}"
    println(JSON.parseObject(json).toJSONString)
  }

  @Test
  def test2(): Unit = {
    val str = "{\"delivery_address\":\"第14大街第35号楼1单元775门\",\"order_comment\":\"描述189124\",\"original_total_amount\":16894.00,\"order_status\":\"1001\",\"consignee_tel\":\"13435798169\",\"trade_body\":\"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 4GB+128GB 明月灰 游戏智能手机 小米 红米等4件商品\",\"id\":53125,\"consignee\":\"邬秋珊\",\"create_time\":\"2021-03-23 12:12:36\",\"expire_time\":\"2021-03-23 12:27:36\",\"coupon_reduce_amount\":0.00,\"out_trade_no\":\"552859813929672\",\"total_amount\":16912.00,\"user_id\":3,\"img_url\":\"http://img.gmall.com/177329.jpg\",\"province_id\":33,\"feight_fee\": null,\"activity_reduce_amount\":0.00}"

    val obj = JSON.parseObject(str)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val createTime = obj.getString("create_time")
    val arr = createTime.split(" ")
    val createDate = arr(0)
    val createHour = arr(1).split(":")(0)
    val createTs = sdf.parse(createTime).getTime
    val orderInfo = OrderInfo(
      obj.getLong("id"),
      obj.getLong("province_id"),
      obj.getString("order_status"),
      obj.getLong("user_id"),
      obj.getBigDecimal("total_amount"),
      obj.getBigDecimal("activity_reduce_amount"),
      obj.getBigDecimal("coupon_reduce_amount"),
      obj.getBigDecimal("original_total_amount"),
      obj.getBigDecimal("feight_fee"),
      obj.getString("expire_time"),
      obj.getString("create_time"),
      obj.getString("operate_time"),
      createDate,
      createHour,
      createTs
    )
    println(orderInfo)
  }

  @Test
  def test3(): Unit ={
    val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val date = LocalDate.parse("2021-01-02", dtf)
    println(date)
    println(date.getYear)

    val dtf2 = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val str = date.format(dtf2)
    println(str)

    val str1 = LocalDateTime.ofEpochSecond(1616678820000L / 1000L, 0, ZoneOffset.ofHours(8)).format(dtf)
    println(str1)

  }

  @Test
  def test4(): Unit ={
    val infoStr = "{\"callback_time\":\"2021-03-26 10:47:17\",\"payment_type\":\"1102\",\"out_trade_no\":\"149472831362414\",\"create_time\":\"2021-03-26 10:46:57\",\"user_id\":29637,\"total_amount\":12394.00,\"subject\":\"小米10 至尊纪念版 双模5G 骁龙865 120HZ高刷新率 120倍长焦镜头 120W快充 8GB+128GB 透明版 游戏手机等5件商品\",\"trade_no\":\"6828188462564891571887527119938499\",\"id\":109813,\"order_id\":156105}"
    val paymentInfo = JSON.parseObject(infoStr, classOf[PaymentInfo])
    println(paymentInfo)

    val orderStr = "{\"activity_reduce_amount\":0.00,\"category3_id\":86,\"category3_name\":\"平板电视\",\"coupon_reduce_amount\":0.00,\"create_date\":\"2021-03-26\",\"create_hour\":\"10\",\"create_time\":\"2021-03-26 10:52:20\",\"create_ts\":1616727140000,\"detail_id\":497351,\"expire_time\":null,\"feight_fee\":5.00,\"operate_time\":null,\"order_id\":157335,\"order_price\":2899.00,\"order_status\":\"1001\",\"original_total_amount\":5960.00,\"province_3166_2_code\":\"CN-TJ\",\"province_area_code\":\"120000\",\"province_id\":2,\"province_iso_code\":\"CN-12\",\"province_name\":\"天津\",\"sku_id\":20,\"sku_name\":\"小米电视E65X 65英寸 全面屏 4K超高清HDR 蓝牙遥控内置小爱 2+8GB AI人工智能液晶网络平板电视 L65M5-EA\",\"sku_num\":2,\"split_activity_amount\":null,\"split_coupon_amount\":null,\"split_feight_fee\":null,\"split_total_amount\":5798.00,\"spu_id\":6,\"spu_name\":\"小米电视 内置小爱 智能网络液晶平板教育电视\",\"tm_id\":5,\"tm_name\":\"小米\",\"total_amount\":5965.00,\"user_age\":53,\"user_gender\":\"F\",\"user_id\":29496}"
    val orderWide = JSON.parseObject(orderStr, classOf[OrderWide])
    val paymentWide = new PaymentWide(paymentInfo, orderWide)
    println(paymentWide)
  }

  @Test
  def test5(): Unit ={
    println(("?" * 24).mkString(","))
  }

  @Test
  def test6(): Unit ={
    val stats = ProductStats()
    val fields = stats.getClass.getDeclaredFields
    val tpe = typeOf[ProductStats]
    fields.foreach{field =>
      val fname = field.getName
      if(fname.equals("order_id_set")){
        println(field.getDeclaredAnnotations.map(_.annotationType().getSimpleName).mkString(","))
      }
    }
  }

  @Test
  def test7(): Unit ={
    val str = "com.example.flink.bean.ProductStats.refund_order_id_set"
  }

}
