package com.example.publish.query.service.impl;

import com.example.publish.query.mapper.mysql.TrademarkStatMapper;
import com.example.publish.query.service.MySQLService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class MySQLServiceImpl implements MySQLService {

    @Autowired
    TrademarkStatMapper trademarkStatMapper;

    @Override
    public List<Map> getTrademardStat(String startDate, String endDate, int topN) {
        return trademarkStatMapper.selectTradeSum(startDate,endDate,topN);
    }
}