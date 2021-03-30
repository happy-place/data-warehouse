select * from user_info;
select * from user_info where id='58621';
select count(1) from user_info;
update user_info set user_level=2 where id=1;

select * from base_province;
update base_province set iso_code='CN-BJ' where id=1;


show tables;
select count(1) from base_trademark;
select count(1) from base_category3;
select count(1) from spu_info;
select count(1) from sku_info;
desc sku_info;
show processlist ;


select order_id,count(1) as cnt from order_detail group by order_id having cnt > 1;


select * from order_detail where order_id='14580';

select * from order_info where id ='14580';

drop table kafka_offset;
CREATE TABLE `kafka_offset` (
                               `group_id` varchar(200) NOT NULL,
                               `topic` varchar(200) NOT NULL,
                               `partition_id` int(11) NOT NULL,
                               `offset` bigint(20) DEFAULT NULL,
                               PRIMARY KEY (`group_id`,`topic`,`partition_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `trademark_amount_stat`
(
    `stat_time` datetime,
    `trademark_id` varchar(20),
    `trademark_name`  varchar(200),
    `amount` decimal(16,2) ,
    PRIMARY KEY (`stat_time`,`trademark_id`,`trademark_name`)
)ENGINE=InnoDB  DEFAULT CHARSET=utf8;

select * from trademark_amount_stat;

select topic,group_id,partition,_offset from kafka_offset where topic='dwd_order_wide' and group_id='trademark_stat_app_group';
select * from kafka_offset where topic='dwd_order_wide' and group_id='trademark_stat_app_group';

select * from kafka_offset;
