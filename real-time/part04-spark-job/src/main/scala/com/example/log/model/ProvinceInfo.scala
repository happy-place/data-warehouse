package com.example.log.model

/**
 * 省份维度表，存储在phoenix，根据业务表变化而变化
 * @param id
 * @param name
 * @param area_code
 * @param iso_code
 */
case class ProvinceInfo (
    id:String,
    name:String,
    area_code:String,
    iso_code:String,
  )