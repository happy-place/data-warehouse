package com.example.log.model

/**
 * 状态表，标记用户是否是首单，存储在hbase，根据事实表数据变化而变化
 * @param userId
 * @param ifConsumed
 */
case class UserStatus(
     userId:String,  //用户id
     ifConsumed:String //是否消费过   0首单   1非首单
)