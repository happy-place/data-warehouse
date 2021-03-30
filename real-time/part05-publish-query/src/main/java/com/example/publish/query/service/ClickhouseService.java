package com.example.publish.query.service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public interface ClickhouseService {

    // 当日总交易额
    BigDecimal selectOrderAmountTotal(String date);

    // 当日分时交易额
    Map<String,BigDecimal> selectOrderAmountHourMap(String date);
}
