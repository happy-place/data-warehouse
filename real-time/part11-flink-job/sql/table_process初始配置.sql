/*
SQLyog 
MySQL - 5.7.29-log : Database - gmall_realtime
*********************************************************************
*/

drop table if exists table_process;

/*!40101 SET NAMES utf8 */;

/*!40101 SET SQL_MODE=''*/;

/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
/*Table structure for table `table_process` */

CREATE TABLE `table_process` (
  `source_table` varchar(200) NOT NULL COMMENT '来源表',
  `operate_type` varchar(200) NOT NULL COMMENT '操作类型 insert,update,delete',
  `sink_type` varchar(200) DEFAULT NULL COMMENT '输出类型 hbase kafka',
  `sink_table` varchar(200) DEFAULT NULL COMMENT '输出表(主题)',
  `sink_columns` varchar(2000) DEFAULT NULL COMMENT '输出字段',
  `sink_pk` varchar(200) DEFAULT NULL COMMENT '主键字段',
  `sink_extend` varchar(200) DEFAULT NULL COMMENT '建表扩展',
  PRIMARY KEY (`source_table`,`operate_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table `table_process` */
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('activity_info', 'insert', 'hbase', 'dim_activity_info', 'id,activity_name,activity_type,activity_desc,start_time,end_time,create_time', 'id', NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('activity_info', 'update', 'hbase', 'dim_activity_info', 'id,activity_name,activity_type,activity_desc,start_time,end_time,create_time', NULL, NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('activity_rule', 'insert', 'hbase', 'dim_activity_rule', 'id,activity_id,activity_type,condition_amount,condition_num,benefit_amount,benefit_discount,benefit_level', 'id', NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('activity_rule', 'update', 'hbase', 'dim_activity_rule', 'id,activity_id,activity_type,condition_amount,condition_num,benefit_amount,benefit_discount,benefit_level', NULL, NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('activity_sku', 'insert', 'hbase', 'dim_activity_sku', 'id,activity_id,sku_id,create_time', 'id', NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('activity_sku', 'update', 'hbase', 'dim_activity_sku', 'id,activity_id,sku_id,create_time', NULL, NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('base_category1', 'insert', 'hbase', 'dim_base_category1', 'id,name', 'id', NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('base_category1', 'update', 'hbase', 'dim_base_category1', 'id,name', NULL, NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('base_category2', 'insert', 'hbase', 'dim_base_category2', 'id,name,category1_id', 'id', NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('base_category2', 'update', 'hbase', 'dim_base_category2', 'id,name,category1_id', NULL, NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('base_category3', 'insert', 'hbase', 'dim_base_category3', 'id,name,category2_id', 'id', NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('base_category3', 'update', 'hbase', 'dim_base_category3', 'id,name,category2_id', NULL, NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('base_dic', 'insert', 'hbase', 'dim_base_dic', 'id,dic_name,parent_code,create_time,operate_time', 'id', NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('base_dic', 'update', 'hbase', 'dim_base_dic', 'id,dic_name,parent_code,create_time,operate_time', NULL, NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('base_province', 'insert', 'hbase', 'dim_base_province', 'id,name,region_id,area_code,iso_code,iso_3166_2', NULL, NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('base_province', 'update', 'hbase', 'dim_base_province', 'id,name,region_id,area_code,iso_code,iso_3166_2', NULL, NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('base_region', 'insert', 'hbase', 'dim_base_region', 'id,region_name', NULL, NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('base_region', 'update', 'hbase', 'dim_base_region', 'id,region_name', NULL, NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('base_trademark', 'insert', 'hbase', 'dim_base_trademark', 'id,tm_name', 'id', NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('base_trademark', 'update', 'hbase', 'dim_base_trademark', 'id,tm_name', NULL, NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('cart_info', 'insert', 'kafka', 'dwd_cart_info', 'id,user_id,sku_id,cart_price,sku_num,img_url,sku_name,is_checked,create_time,operate_time,is_ordered,order_time,source_type,source_id', 'id', NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('comment_info', 'insert', 'kafka', 'dwd_comment_info', 'id,user_id,nick_name,head_img,sku_id,spu_id,order_id,appraise,comment_txt,create_time,operate_time', 'id', NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('coupon_info', 'insert', 'hbase', 'dim_coupon_info', 'id,coupon_name,coupon_type,condition_amount,condition_num,activity_id,benefit_amount,benefit_discount,create_time,range_type,limit_num,taken_count,start_time,end_time,operate_time,expire_time,range_desc', 'id', NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('coupon_info', 'update', 'hbase', 'dim_coupon_info', 'id,coupon_name,coupon_type,condition_amount,condition_num,activity_id,benefit_amount,benefit_discount,create_time,range_type,limit_num,taken_count,start_time,end_time,operate_time,expire_time,range_desc', NULL, NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('coupon_range', 'insert', 'hbase', 'dim_coupon_range', 'id,coupon_id,range_type,range_id', 'id', NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('coupon_range', 'update', 'hbase', 'dim_coupon_range', 'id,coupon_id,range_type,range_id', NULL, NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('coupon_use', 'insert', 'kafka', 'dwd_coupon_use', 'id,coupon_id,user_id,order_id,coupon_status,get_type,get_time,using_time,used_time,expire_time', 'id', ' SALT_BUCKETS = 3');
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('coupon_use', 'update', 'kafka', 'dwd_coupon_use', 'id,coupon_id,user_id,order_id,coupon_status,get_type,get_time,using_time,used_time,expire_time', NULL, NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('favor_info', 'insert', 'kafka', 'dwd_favor_info', 'id,user_id,sku_id,spu_id,is_cancel,create_time,cancel_time', 'id', NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('financial_sku_cost', 'insert', 'hbase', 'dim_financial_sku_cost', 'id,sku_id,sku_name,busi_date,is_lastest,sku_cost,create_time', 'id', NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('financial_sku_cost', 'update', 'hbase', 'dim_financial_sku_cost', 'id,sku_id,sku_name,busi_date,is_lastest,sku_cost,create_time', NULL, NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('order_detail', 'insert', 'kafka', 'dwd_order_detail', 'id,order_id,sku_id,sku_name,order_price,sku_num,create_time,source_type,source_id,split_activity_amount,split_coupon_amount,split_total_amount', 'id', NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('order_detail_activity', 'insert', 'kafka', 'dwd_order_detail_activity', 'id,order_id,order_detail_id,activity_id,activity_rule_id,sku_id,create_time', 'id', NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('order_detail_coupon', 'insert', 'kafka', 'dwd_order_detail_coupon', 'id,order_id,order_detail_id,coupon_id,coupon_use_id,sku_id,create_time', 'id', NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('order_info', 'insert', 'kafka', 'dwd_order_info', 'id,consignee,consignee_tel,total_amount,order_status,user_id,payment_way,delivery_address,order_comment,out_trade_no,trade_body,create_time,operate_time,expire_time,process_status,tracking_no,parent_order_id,img_url,province_id,activity_reduce_amount,coupon_reduce_amount,original_total_amount,feight_fee,feight_fee_reduce,refundable_time', 'id', NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('order_info', 'update', 'kafka', 'dwd_order_info_update', 'id,consignee,consignee_tel,total_amount,order_status,user_id,payment_way,delivery_address,order_comment,out_trade_no,trade_body,create_time,operate_time,expire_time,process_status,tracking_no,parent_order_id,img_url,province_id,activity_reduce_amount,coupon_reduce_amount,original_total_amount,feight_fee,feight_fee_reduce,refundable_time', NULL, NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('order_refund_info', 'insert', 'kafka', 'dwd_order_refund_info', 'id,user_id,order_id,sku_id,refund_type,refund_num,refund_amount,refund_reason_type,refund_reason_txt,refund_status,create_time', 'id', NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('payment_info', 'insert', 'kafka', 'dwd_payment_info', 'id,out_trade_no,order_id,user_id,payment_type,trade_no,total_amount,subject,payment_status,create_time,callback_time,callback_content', 'id', NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('payment_info', 'update', 'kafka', 'dwd_payment_info', 'id,out_trade_no,order_id,user_id,payment_type,trade_no,total_amount,subject,payment_status,create_time,callback_time,callback_content', NULL, NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('refund_payment', 'insert', 'kafka', 'dwd_refund_payment', 'id,out_trade_no,order_id,sku_id,payment_type,trade_no,total_amount,subject,refund_status,create_time,callback_time,callback_content', 'id', NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('refund_payment', 'update', 'kafka', 'dwd_refund_payment', 'id,out_trade_no,order_id,sku_id,payment_type,trade_no,total_amount,subject,refund_status,create_time,callback_time,callback_content', NULL, NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('sku_info', 'insert', 'hbase', 'dim_sku_info', 'id,spu_id,price,sku_name,sku_desc,weight,tm_id,category3_id,sku_default_img,is_sale,create_time', 'id', ' SALT_BUCKETS = 4');
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('sku_info', 'update', 'hbase', 'dim_sku_info', 'id,spu_id,price,sku_name,sku_desc,weight,tm_id,category3_id,sku_default_img,is_sale,create_time', NULL, NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('spu_info', 'insert', 'hbase', 'dim_spu_info', 'id,spu_name,description,category3_id,tm_id', 'id', ' SALT_BUCKETS = 3');
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('spu_info', 'update', 'hbase', 'dim_spu_info', 'id,spu_name,description,category3_id,tm_id', NULL, NULL);
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('user_info', 'insert', 'hbase', 'dim_user_info', 'id,login_name,name,user_level,birthday,gender,create_time,operate_time', 'id', ' SALT_BUCKETS = 3');
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('user_info', 'update', 'hbase', 'dim_user_info', 'id,login_name,name,user_level,birthday,gender,create_time,operate_time', NULL, NULL);
