drop schema if exists GMALL_FLINK;
CREATE SCHEMA IF NOT EXISTS GMALL_FLINK;
select * from GMALL_FLINK.DIM_BASE_CATEGORY1;

delete from GMALL_FLINK.DIM_BASE_CATEGORY1 where id is not null;


select * from GMALL_FLINK.DIM_BASE_CATEGORY1;
select * from GMALL_FLINK.DIM_BASE_CATEGORY1 where id = '11';
show tables;


select * from GMALL_FLINK.DIM_USER_INFO;

select * from GMALL_FLINK.DIM_USER_INFO where ID = 536

select * from GMALL_FLINK.DIM_USER_INFO where ID = '23';
select * from GMALL_FLINK.DIM_USER_INFO where ID = '1725';

select * from GMALL_FLINK.DIM_USER_INFO where ID = '1725';
select * from DIM_BASE_PROVINCE;

select * from DIM_SPU_INFO;

select * from DIM_SKU_INFO;
select * from DIM_BASE_CATEGORY3;
select * from DIM_BASE_TRADEMARK;

select * from GMALL_FLINK.DIM_BASE_TRADEMARK where ID = '10';
SELECT * FROM GMALL_FLINK.DIM_BASE_TRADEMARK WHERE ID = '3';

SELECT * FROM GMALL_FLINK.DIM_SKU_INFO WHERE ID = '5';

