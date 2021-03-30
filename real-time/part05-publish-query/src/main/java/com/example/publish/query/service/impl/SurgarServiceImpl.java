package com.example.publish.query.service.impl;

import com.example.publish.query.bean.KeywordStats;
import com.example.publish.query.bean.ProductStats;
import com.example.publish.query.bean.ProvinceStats;
import com.example.publish.query.bean.VisitorStats;
import com.example.publish.query.mapper.clickhouse.SurgarMapper;
import com.example.publish.query.service.SurgarService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

@Service
class SurgarServiceImpl implements SurgarService {

    @Autowired
    private SurgarMapper surgarMapper;

    @Override
    public BigDecimal getGMV(int date) {
        return surgarMapper.getGMV(date);
    }


    @Override
    public List<ProductStats> getProductStatsGroupBySpu(int date, int limit) {
        return surgarMapper.getProductStatsGroupBySpu(date,limit);
    }

    @Override
    public List<ProductStats> getProductStatsGroupByCategory3(int date, int limit) {
        return surgarMapper.getProductStatsGroupByCategory3(date,limit);
    }

    @Override
    public List<ProductStats> getProductStatsByTrademark(int date, int limit) {
        return surgarMapper.getProductStatsByTrademark(date,limit);
    }

    @Override
    public List<ProvinceStats> getProvinceStats(int date) {
        return surgarMapper.selectProvinceStats(date);
    }


    @Override
    public List<VisitorStats> getVisitorStatsByNewFlag(int date) {
        return surgarMapper.selectVisitorStatsByNewFlag(date);
    }

    @Override
    public List<VisitorStats> getVisitorStatsByHour(int date) {
        return surgarMapper.selectVisitorStatsByHour(date);
    }

    @Override
    public Long getPv(int date) {
        return surgarMapper.selectPv(date);
    }

    @Override
    public Long getUv(int date) {
        return surgarMapper.selectUv(date);
    }

    @Override
    public List<KeywordStats> getKeywordStats(int date, int limit) {
        return surgarMapper.selectKeywordStats(date,limit);
    }
}
