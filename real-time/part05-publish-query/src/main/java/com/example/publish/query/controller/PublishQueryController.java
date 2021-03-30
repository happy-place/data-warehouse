package com.example.publish.query.controller;

import com.example.publish.query.service.ClickhouseService;
import com.example.publish.query.service.ESService;
import com.example.publish.query.service.MySQLService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublishQueryController {

    @Autowired
    private ESService esService;

    @Autowired
    private ClickhouseService clickhouseService;

    @Autowired
    private MySQLService mysqlService;

    @GetMapping("ping")
    public Object ping(){
        return "pong";
    }

    @GetMapping("realtime-total")
    public Object realtimeTotal(@RequestParam(required = true,name="date") String date){
        List<Map<String,Object>> result = new ArrayList<>();
        Map<String,Object> dauMap = new HashMap<>();
        Long dauTotal = esService.getDauTotal(date);
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal!=null?dauTotal:0L);

        // TODO
        result.add(dauMap);
        Map<String,Object> newMidMap = new HashMap<>();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",233);
        result.add(newMidMap);

        Map<String,Object> orderAmountMap = new HashMap<>();
        BigDecimal bigDecimal = clickhouseService.selectOrderAmountTotal(date);
        orderAmountMap.put("id","order_amount");
        orderAmountMap.put("name","新增交易额");
        orderAmountMap.put("value", bigDecimal);
        result.add(orderAmountMap);

        return result;
    }

    @GetMapping("realtime-hour")
    public Object realtimeHour(@RequestParam(value = "id",defaultValue = "-1") String id,@RequestParam(required = true,name="date") String date){
        Map<String, Map<String, Object>> result = new HashMap<>();
        if("dau".equals(id)){
            Map todayMap = esService.getDauHour(date);
            result.put("today",todayMap);
            String yesterday = getDate(date, -1);
            Map yesterdayMap = esService.getDauHour(yesterday);
            result.put("yesterday",yesterdayMap);
        }else if("order_amount".equals(id)){
            Map todayMap = clickhouseService.selectOrderAmountHourMap(date);
            result.put("today",todayMap);
            String yesterday = getDate(date, -1);
            Map yesterdayMap = clickhouseService.selectOrderAmountHourMap(yesterday);
            result.put("yesterday",yesterdayMap);
        }else{
            result = null;
        }
        return result;
    }


    @GetMapping("trademark-sum")
    public Object trademarkSum(@RequestParam("start_date") String startDate,
                               @RequestParam("end_date") String endDate,
                               @RequestParam("topN") int topN){

        List<Map> trademardSum = mysqlService.getTrademardStat(startDate, endDate,topN);
        return trademardSum ;
    }

    @GetMapping("trademark-datav")
    public Object trademarkSumDatav(@RequestParam("start_date") String startDate,
                               @RequestParam("end_date") String endDate,
                               @RequestParam("topN") int topN){
        List<Map> trademarkSumList = mysqlService.getTrademardStat(startDate, endDate,topN);
        //根据DataV图形数据要求进行调整，  x :品牌 ,y 金额， s 1
        List<Map> datavList=new ArrayList<>();
        for (Map trademardSumMap : trademarkSumList) {
            Map  map = new HashMap<>();
            map.put("x",trademardSumMap.get("trademark_name"));
            map.put("y",trademardSumMap.get("amount"));
            map.put("s",1);
            datavList.add(map);
        }
        return datavList;
    }


    private String getDate(String dateStr,int days){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String targetDateStr = null;
        try {
            Date date = sdf.parse(dateStr);
            Date targetDate = DateUtils.addDays(date, days);
            targetDateStr = sdf.format(targetDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return targetDateStr;
    }


}
