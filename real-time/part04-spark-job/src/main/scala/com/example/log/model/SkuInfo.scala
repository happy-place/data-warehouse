package com.example.log.model

/**
 * 商品库存维表
 * @param id
 * @param spu_id
 * @param price
 * @param sku_name
 * @param tm_id
 * @param category3_id
 * @param create_time
 * @param category3_name
 * @param spu_name
 * @param tm_name
 */
case class SkuInfo(id:String ,
                   spu_id:String ,
                   price:String ,
                   sku_name:String ,
                   tm_id:String ,
                   category3_id:String ,
                   create_time:String,

                   var category3_name:String,
                   var spu_name:String,
                   var tm_name:String)