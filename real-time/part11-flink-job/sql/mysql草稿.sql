drop database gmall_flink;
create database gmall_flink;

show tables from gmall_flink;

create database config;

use gmall_flink;
CREATE TABLE config.`table_process` (
  `source_table` varchar(200) NOT NULL COMMENT '来源表',
  `operate_type` varchar(200) NOT NULL COMMENT '操作类型 insert,update,delete',
   `sink_type` varchar(200) DEFAULT NULL COMMENT '输出类型 hbase kafka',
  `sink_table` varchar(200) DEFAULT NULL COMMENT '输出表(主题)',
  `sink_columns` varchar(2000) DEFAULT NULL COMMENT '输出字段',
  `sink_pk` varchar(200) DEFAULT NULL COMMENT '主键字段',
  `sink_extend` varchar(200) DEFAULT NULL COMMENT '建表扩展',
  PRIMARY KEY (`source_table`,`operate_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


insert into table_process values('base_province','insert','hbase','dim_base_trademark','id,name,region_id,area_code,iso_code','id','');
INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('order_info', 'insert', 'kafka', 'dwd_order_info', 'id,consignee,consignee_tel,total_amount,order_status,user_id,payment_way,delivery_address,order_comment,out_trade_no,trade_body,create_time,operate_time,expire_time,process_status,tracking_no,parent_order_id,img_url,province_id,activity_reduce_amount,coupon_reduce_amount,original_total_amount,feight_fee,feight_fee_reduce,refundable_time', 'id', NULL);

show tables from gmall_flink;
select * from table_process;

select * from gmall_flink.user_info;
update gmall_flink.user_info set user_level=3 where id=1;

select * from config.table_process;

truncate table table_process;

INSERT INTO `table_process`(`source_table`, `operate_type`, `sink_type`, `sink_table`, `sink_columns`, `sink_pk`, `sink_extend`) VALUES ('base_category1', 'insert', 'hbase', 'dim_base_category1', 'id,name', 'id', NULL);

# 测试写入phoenix
select * from gmall_flink.base_category1;

insert into gmall_flink.base_category1(id, name) value(18,'aa');
insert into gmall_flink.base_category1(id, name) value(19,'aa');
insert into gmall_flink.base_category1(id, name) value(20,'aa');
insert into gmall_flink.base_category1(id, name) value(21,'aa');

# 测试写入kafka
select * from gmall_flink.order_info;


select * from gmall_flink.base_category1 where id = '11';
update gmall_flink.base_category1 set name='鞋靴' where id = '11';

desc gmall_flink.user_info;

select distinct sink_table from config.table_process where sink_table like 'DIM%';


select * from gmall_flink.favor_info;
select * from gmall_flink.refund_payment;


