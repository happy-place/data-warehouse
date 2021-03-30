package com.example.log.model

/**
 * 用户维度表，存储在phoenix
 * @param id
 * @param user_level
 * @param birthday
 * @param gender
 * @param age_group
 * @param gender_name
 */
case class UserInfo(
   id:String,
   user_level:String,
   birthday:String,
   gender:String,
   var age_group:String,//年龄段
   var gender_name:String
 ) //性别