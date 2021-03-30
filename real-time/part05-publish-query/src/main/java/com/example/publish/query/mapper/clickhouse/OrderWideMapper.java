package com.example.publish.query.mapper.clickhouse;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public interface OrderWideMapper {

    // 当日总交易额
    BigDecimal selectOrderAmountTotal(String date);

    // 当日分时交易额
    List<Map<String,BigDecimal>> selectOrderAmountHourMap(String date);
}
