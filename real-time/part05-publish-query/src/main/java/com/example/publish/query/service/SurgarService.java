package com.example.publish.query.service;

import com.example.publish.query.bean.KeywordStats;
import com.example.publish.query.bean.ProductStats;
import com.example.publish.query.bean.ProvinceStats;
import com.example.publish.query.bean.VisitorStats;

import java.math.BigDecimal;
import java.util.List;

public interface SurgarService {

    // 统计当日商品总交易额
    BigDecimal getGMV(int date);

    //统计某天不同SPU商品交易额排名
    List<ProductStats> getProductStatsGroupBySpu(int date, int limit);

    //统计某天不同类别商品交易额排名
    List<ProductStats> getProductStatsGroupByCategory3(int date,int limit);

    //统计某天不同品牌商品交易额排名
    List<ProductStats> getProductStatsByTrademark(int date,int limit);

    //统计某天不同省份商品交易额
    List<ProvinceStats> getProvinceStats(int date);

    // 新用户访问习惯统计
    List<VisitorStats> getVisitorStatsByNewFlag(int date);

    // 当日用户分时访问习惯统计
    List<VisitorStats> getVisitorStatsByHour(int date);

    // 当日访问pv
    Long getPv(int date);

    // 当日访问uv
    Long getUv(int date);

    // 关键词评分统计
    List<KeywordStats> getKeywordStats(int date, int limit);
    
}
