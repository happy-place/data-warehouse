drop table t_order_wide;
create table t_order_wide(
                                   order_detail_id UInt64,
                                   order_id  UInt64,
                                   order_status String,
                                   create_time DateTime,
                                   user_id UInt64,
                                   sku_id  UInt64,
                                   sku_price Decimal64(2),
                                   sku_num  UInt64,
                                   sku_name  String,
                                   benefit_reduce_amount Decimal64(2),
                                   original_total_amount Decimal64(2),
                                   feight_fee Decimal64(2),
                                   final_total_amount  Decimal64(2),
                                   final_detail_amount Decimal64(2),
                                   if_first_order String,
                                   province_name String,
                                   province_area_code String,
                                   user_age_group String,
                                   user_gender String,
                                   dt Date,
                                   spu_id  UInt64,
                                   tm_id  UInt64,
                                   category3_id  UInt64,
                                   spu_name  String,
                                   tm_name  String,
                                   category3_name  String
)engine =ReplacingMergeTree(create_time)
     partition by dt
     primary key (order_detail_id)
     order by (order_detail_id );


select * from default.t_order_wide;

select count(1) from default.t_order_wide;