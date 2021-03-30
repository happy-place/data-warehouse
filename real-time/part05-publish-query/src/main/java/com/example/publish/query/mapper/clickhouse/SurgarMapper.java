package com.example.publish.query.mapper.clickhouse;

import com.example.publish.query.bean.KeywordStats;
import com.example.publish.query.bean.ProductStats;
import com.example.publish.query.bean.ProvinceStats;
import com.example.publish.query.bean.VisitorStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

/**
 * ${} 传数据库对象（表名、列名），不会预编译，存在SQL注入风险
 * #{} 传参，使用预编译
 *
 */

public interface SurgarMapper {

    /**
     * 获取当日中交易额
     * 只有一个参数时，直接传，不需要使用@Param
     * @param date
     * @return
     */
    @Select("select sum(order_amount) order_amount " +
            "from dws_product_stats " +
            "where toYYYYMMDD(stt)=#{date}")
    BigDecimal getGMV(int date);

    /**
     * 统计当日各SPU商品交易额排名 topN
     * 传入多个参数时，需要使用@Param 声明
     * @param date
     * @param limit
     * @return
     */
    @Select("select spu_id,spu_name,sum(order_amount) order_amount,sum(order_ct) order_ct " +
            "from dws_product_stats where toYYYYMMDD(stt)=#{date} " +
            "group by spu_id,spu_name having order_amount>0 " +
            "order by order_amount desc limit #{limit}")
    List<ProductStats> getProductStatsGroupBySpu(@Param("date") int date, @Param("limit") int limit);

    /**
     * 统计当日各类别商品交易额排名 topN
     * @param date
     * @param limit
     * @return
     */
    @Select("select category3_id,category3_name,sum(order_amount) order_amount " +
            "from dws_product_stats where toYYYYMMDD(stt)=#{date} " +
            "group by category3_id,category3_name having order_amount > 0 " +
            "order by order_amount desc limit #{limit}")
    List<ProductStats> getProductStatsGroupByCategory3(@Param("date") int date, @Param("limit") int limit);

    /**
     * 统计当日个品牌商品交易额排名 topN
     * @param date
     * @param limit
     * @return
     */
    @Select("select tm_id,tm_name,sum(order_amount) order_amount " +
            "from dws_product_stats where toYYYYMMDD(stt)=#{date} " +
            "group by tm_id,tm_name having order_amount > 0 " +
            "order by order_amount desc limit #{limit}")
    List<ProductStats> getProductStatsByTrademark(@Param("date") int date, @Param("limit") int limit);

    //按地区查询交易额
    @Select("select province_name,sum(order_amount) order_amount " +
            "from dws_province_stats where toYYYYMMDD(stt)=#{date} " +
            "group by province_id ,province_name")
    List<ProvinceStats> selectProvinceStats(int date);


    //新老访客流量统计
    @Select("select is_new,sum(uv_ct) uv_ct,sum(pv_ct) pv_ct," +
            "sum(sv_ct) sv_ct, sum(uj_ct) uj_ct,sum(dur_sum) dur_sum " +
            "from dws_visitor_stats where toYYYYMMDD(stt)=#{date} group by is_new")
    List<VisitorStats> selectVisitorStatsByNewFlag(int date);

    //分时流量统计
    @Select("select sum(if(is_new='1', dws_visitor_stats.uv_ct,0)) new_uv,toHour(stt) hr," +
            "sum(dws_visitor_stats.uv_ct) uv_ct, sum(pv_ct) pv_ct, sum(uj_ct) uj_ct  " +
            "from dws_visitor_stats where toYYYYMMDD(stt)=#{date} group by toHour(stt)")
    List<VisitorStats> selectVisitorStatsByHour(int date);

    // 每日pv
    @Select("select count(pv_ct) pv_ct from dws_visitor_stats " +
            "where toYYYYMMDD(stt)=#{date}  ")
    Long selectPv(int date);

    // 每日uv
    @Select("select count(uv_ct) uv_ct from dws_visitor_stats " +
            "where toYYYYMMDD(stt)=#{date}  ")
    Long selectUv(int date);

    // 关键词评分统计，multiIf相当于case when then
    @Select("select keyword," +
            "sum(dws_keyword_stats.ct * " +
            "multiIf(source='SEARCH',10,source='ORDER',3,source='CART',2,source='CLICK',1,0)) ct" +
            " from dws_keyword_stats where toYYYYMMDD(stt)=#{date} group by keyword " +
            "order by sum(dws_keyword_stats.ct) desc limit #{limit} ")
    List<KeywordStats> selectKeywordStats(@Param("date") int date, @Param("limit") int limit);

}
