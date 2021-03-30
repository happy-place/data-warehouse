package com.example.publish.query.service.impl;

import com.example.publish.query.mapper.clickhouse.OrderWideMapper;
import com.example.publish.query.service.ClickhouseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ClickhouseServiceImpl implements ClickhouseService {

    @Autowired
    private OrderWideMapper orderWideMapper;

    @Override
    public BigDecimal selectOrderAmountTotal(String date) {
        BigDecimal bigDecimal = orderWideMapper.selectOrderAmountTotal(date);
        return bigDecimal;
    }

    @Override
    public Map<String,BigDecimal> selectOrderAmountHourMap(String date) {
        Map<String,BigDecimal> result = new HashMap<>();
        List<Map<String,BigDecimal>> temp = orderWideMapper.selectOrderAmountHourMap(date);
        for (Map map : temp) {
            // 0~9 转换为 00 制
            result.put(String.format("%02d",map.get("hr")),(BigDecimal) map.get("am"));
        }
        return result;
    }
}
