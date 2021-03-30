show tables;

set global validate_password_length=4;
set global validate_password_policy=0;
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%' IDENTIFIED BY 'canal' ;

show databases;

desc activity_info;

desc activity_info;
desc activity_order;
desc activity_rule;
desc activity_sku;
desc base_category1;
desc base_category2;
desc base_category3;
desc base_dic;
desc base_province;
desc base_region;
desc base_trademark;
desc cart_info;
desc comment_info;
desc coupon_info;
desc coupon_use;
desc date_info;
desc favor_info;
desc holiday_info;
desc holiday_year;
desc order_detail;
desc order_info;
desc order_refund_info;
desc order_status_log;
desc payment_info;
desc sku_info;
desc spu_info;
desc user_info;
desc z_user_info;
