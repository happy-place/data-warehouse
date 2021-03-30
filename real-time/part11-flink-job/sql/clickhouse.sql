drop table dws_visitor_stats;
create table dws_visitor_stats
(
    stt     DateTime,
    edt     DateTime,
    vc      String,
    ch      String,
    ar      String,
    is_new  String,
    uv_ct   UInt64,
    pv_ct   UInt64,
    sv_ct   UInt64,
    uj_ct   UInt64,
    dur_sum UInt64,
    ts      UInt64
) engine = ReplacingMergeTree(ts)
      partition by toYYYYMMDD(stt)
      order by (stt, edt, is_new, vc, ch, ar);

select *
from dws_visitor_stats;

-- rollup 从右往左逐一去掉维度
select ar,
       ch,
       vc,
       is_new,
       sum(uv_ct),
       sum(pv_ct),
       sum(sv_ct),
       sum(uj_ct),
       sum(dur_sum)
from dws_visitor_stats
group by ar, ch, vc, is_new
with rollup;

-- cube 先从右往左，然后从左往右逐一去掉维度
select ar,
       ch,
       vc,
       is_new,
       sum(uv_ct),
       sum(pv_ct),
       sum(sv_ct),
       sum(uj_ct),
       sum(dur_sum)
from dws_visitor_stats
group by ar, ch, vc, is_new
with CUBE;

-- totals，返回group by 和汇总结果
select ar,
       ch,
       vc,
       is_new,
       sum(uv_ct),
       sum(pv_ct),
       sum(sv_ct),
       sum(uj_ct),
       sum(dur_sum)
from dws_visitor_stats
group by ar, ch, vc, is_new
with TOTALS;

create table dws_product_stats
(
    stt             DateTime,
    edt             DateTime,
    sku_id          UInt64,
    sku_name        String,
    sku_price       Decimal64(2),
    spu_id          UInt64,
    spu_name        String,
    tm_id           UInt64,
    tm_name         String,
    category3_id    UInt64,
    category3_name  String,
    display_ct      UInt64,
    click_ct        UInt64,
    favor_ct        UInt64,
    cart_ct         UInt64,
    order_sku_num   UInt64,
    order_amount    Decimal64(2),
    order_ct        UInt64,
    payment_amount  Decimal64(2),
    paid_order_ct   UInt64,
    refund_order_ct UInt64,
    refund_amount   Decimal64(2),
    comment_ct      UInt64,
    good_comment_ct UInt64,
    ts              UInt64
) engine = ReplacingMergeTree(ts)
      partition by toYYYYMMDD(stt)
      order by (stt, edt, sku_id);

select count(1)
from dws_product_stats;

select *
from dws_product_stats
order by ts desc;

create table dws_province_stats
(
    stt           DateTime,
    edt           DateTime,
    province_id   UInt64,
    province_name String,
    area_code     String,
    iso_code      String,
    iso_3166_2    String,
    order_amount  Decimal64(2),
    order_count   UInt64,
    ts            UInt64
) engine = ReplacingMergeTree(ts)
      partition by toYYYYMMDD(stt)
      order by (stt, edt, province_id);

select *
from dws_province_stats;


create table dws_keyword_stats
(
    stt     DateTime,
    edt     DateTime,
    keyword String,
    source  String,
    ct      UInt64,
    ts      UInt64
) engine = ReplacingMergeTree(ts)
      partition by toYYYYMMDD(stt)
      order by (stt, edt, keyword, source);

select * from dws_keyword_stats;

select toYYYYMMDD(stt),* from dws_product_stats;
select sum(order_amount) order_amount from dws_product_stats where toYYYYMMDD(stt)=20210328;


select * from dws_visitor_stats;

select province_name,sum(order_amount) order_amount from dws_province_stats where toYYYYMMDD(stt)=20210329 group by province_id ,province_name;
select province_id,province_name,sum(order_amount) order_amount
from dws_province_stats
where toYYYYMMDD(stt)=20210328 group by province_id ,province_name;

select * from dws_province_stats;