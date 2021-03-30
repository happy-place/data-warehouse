package com.example.publish.query.controller;

import com.alibaba.fastjson.JSON;
import com.example.publish.query.bean.KeywordStats;
import com.example.publish.query.bean.ProductStats;
import com.example.publish.query.bean.ProvinceStats;
import com.example.publish.query.bean.VisitorStats;
import com.example.publish.query.service.SurgarService;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.*;

@RestController
@RequestMapping("/api/sugar")
public class SugarController {

    @Autowired
    private SurgarService surgarService;

    private Integer now(){
        return Integer.parseInt(DateFormatUtils.format(new Date(),"yyyyMMdd"));
    }

    /**
     * 获取一天的总交易额
     * {
     *   "date": "20210328",
     *   "data": 8440797.36,
     *   "status": 0
     * }
     * @param date
     * @return
     */
    @RequestMapping("/gmv")
    public Object getGVM(@RequestParam(name="date",required = false)Integer date){
        Map<String,Object> result = new HashMap<>();
        if(date == null){
            date = now();
        }
        result.put("status",0);
        result.put("date",date);
        result.put("data", surgarService.getGMV(date));
        return result;
    }

    /**
     * 当日商品交易 topN
     * {
     *     "status": 0,
     *     "data": {
     *         "columns": [
     *             { "name": "商品名称",  "id": "spu_name"
     *             },
     *             { "name": "交易额", "id": "order_amount"
     *             }
     *         ],
     *         "rows": [
     *             {
     *                 "spu_name": "小米10",
     *                 "order_amount": "863399.00"
     *             },
     *            {
     *                 "spu_name": "iPhone11",
     *                 "order_amount": "548399.00"
     *             }
     *         ]
     *     }
     * }
     * @param date
     * @return
     */
    @RequestMapping("/spu")
    public Object getProductStatsGroupBySpu(@RequestParam(value = "date", required = false) Integer date,
            @RequestParam(value = "limit", defaultValue = "10") int limit) {
        if (date == null){
            date = now();
        }
        List<ProductStats> statsList = surgarService.getProductStatsGroupBySpu(date, limit);
        StringBuilder jsonBuilder =
                new StringBuilder(" " +
                        "{\"status\":0,\"data\":{\"columns\":[" +
                        "{\"name\":\"商品名称\",\"id\":\"spu_name\"}," +
                        "{\"name\":\"交易额\",\"id\":\"order_amount\"}," +
                        "{\"name\":\"订单数\",\"id\":\"order_ct\"}]," +
                        "\"rows\":[");
        //循环拼接表体
        for (int i = 0; i < statsList.size(); i++) {
            ProductStats productStats = statsList.get(i);
            if (i >= 1) {
                jsonBuilder.append(",");
            }
            jsonBuilder.append("{\"spu_name\":\"" + productStats.getSpu_name() + "\"," +
                    "\"order_amount\":" + productStats.getOrder_amount() + "," +
                    "\"order_ct\":" + productStats.getOrder_ct() + "}");

        }
        jsonBuilder.append("]}}");
        return JSON.parseObject(jsonBuilder.toString());
    }

    /**
     * 当日品类交易 topN
     * {
     *     "status": 0,
     *     "data": [
     *         {
     *             "name": "数码类",
     *             "value": 371570
     *         },
     *         {
     *             "name": "日用品",
     *             "value": 296016
     *         }
     *     ]
     * }
     *
     */
    @RequestMapping("/category3")
    public Object getProductStatsGroupByCategory3(
            @RequestParam(value = "date", required = false) Integer date,
            @RequestParam(value = "limit", defaultValue = "4") int limit) {
        if (date == null) {
            date = now();
        }
        List<ProductStats> statsList
                = surgarService.getProductStatsGroupByCategory3(date, limit);

        StringBuilder dataJson = new StringBuilder("{  \"status\": 0, \"data\": [");
        int i = 0;
        for (ProductStats productStats : statsList) {
            if (i++ > 0) {
                dataJson.append(",");
            }
            ;
            dataJson.append("{\"name\":\"")
                    .append(productStats.getCategory3_name()).append("\",");
            dataJson.append("\"value\":")
                    .append(productStats.getOrder_amount()).append("}");
        }
        dataJson.append("]}");
        return JSON.parseObject(dataJson.toString());
    }

    /**
     * 当日品牌交易 topN
     *  {
     *      "status": 0,
     *      "data": {
     *          "categories": [
     *              "三星","vivo","oppo"
     *          ],
     *          "series": [
     *              {
     *                  "data": [ 406333, 709174, 681971
     *                  ]
     *              }
     *          ]
     *      }
     *     }
     */
    @RequestMapping("/trademark")
    public Object getProductStatsByTrademark(
            @RequestParam(value = "date", required = false) Integer date,
            @RequestParam(value = "limit", defaultValue = "20") int limit) {
        if (date == null) {
            date = now();
        }
        List<ProductStats> productStatsByTrademarkList
                = surgarService.getProductStatsByTrademark(date, limit);
        List<String> tradeMarkList = new ArrayList<>();
        List<BigDecimal> amountList = new ArrayList<>();
        for (ProductStats productStats : productStatsByTrademarkList) {
            tradeMarkList.add(productStats.getTm_name());
            amountList.add(productStats.getOrder_amount());
        }
        String json = "{\"status\":0,\"data\":{" + "\"categories\":" +
                "[\"" + StringUtils.join(tradeMarkList, "\",\"") + "\"],\"series\":[" +
                "{\"data\":[" + StringUtils.join(amountList, ",") + "]}]}}";
        return JSON.parseObject(json);
    }

    /**
     * 当日省份交易topN
     * {
     *     "status": 0,
     *     "data": {
     *         "mapData": [
     *             {
     *                 "name": "北京",
     *                 "value": 9131
     *             },
     *             {
     *                 "name": "天津",
     *                 "value": 5740
     *            }
     *        ]
     *     }
     * }
     * @param date
     * @return
     */
    @RequestMapping("/province")
    public Object getProvinceStats(@RequestParam(value = "date",required = false) Integer date) {
        if (date == null) {
            date = now();
        }

        StringBuilder jsonBuilder = new StringBuilder("{\"status\":0,\"data\":{\"mapData\":[");
        List<ProvinceStats> provinceStatsList = surgarService.getProvinceStats(date);
        if (provinceStatsList.size() == 0) {
            //    jsonBuilder.append(  "{\"name\":\"北京\",\"value\":0.00}");
        }
        for (int i = 0; i < provinceStatsList.size(); i++) {
            if (i >= 1) {
                jsonBuilder.append(",");
            }
            ProvinceStats provinceStats = provinceStatsList.get(i);
            jsonBuilder.append("{\"name\":\"" + provinceStats.getProvince_name() + "\",\"value\":" + provinceStats.getOrder_amount() + " }");

        }
        jsonBuilder.append("]}}");
        return JSON.parseObject(jsonBuilder.toString());
    }

    @RequestMapping("/visitor")
    public Object getVisitorStatsByNewFlag(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) date = now();
        List<VisitorStats> visitorStatsByNewFlag = surgarService.getVisitorStatsByNewFlag(date);
        VisitorStats newVisitorStats = new VisitorStats();
        VisitorStats oldVisitorStats = new VisitorStats();
        //循环把数据赋给新访客统计对象和老访客统计对象
        for (VisitorStats visitorStats : visitorStatsByNewFlag) {
            if (visitorStats.getIs_new().equals("1")) {
                newVisitorStats = visitorStats;
            } else {
                oldVisitorStats = visitorStats;
            }
        }
        //把数据拼接入字符串
        String json = "{\"status\":0,\"data\":{\"combineNum\":1,\"columns\":" +
                "[{\"name\":\"类别\",\"id\":\"type\"}," +
                "{\"name\":\"新用户\",\"id\":\"new\"}," +
                "{\"name\":\"老用户\",\"id\":\"old\"}]," +
                "\"rows\":" +
                "[{\"type\":\"用户数(人)\"," +
                "\"new\": " + newVisitorStats.getUv_ct() + "," +
                "\"old\":" + oldVisitorStats.getUv_ct() + "}," +
                "{\"type\":\"总访问页面(次)\"," +
                "\"new\":" + newVisitorStats.getPv_ct() + "," +
                "\"old\":" + oldVisitorStats.getPv_ct() + "}," +
                "{\"type\":\"跳出率(%)\"," +
                "\"new\":" + newVisitorStats.getUjRate() + "," +
                "\"old\":" + oldVisitorStats.getUjRate() + "}," +
                "{\"type\":\"平均在线时长(秒)\"," +
                "\"new\":" + newVisitorStats.getDurPerSv() + "," +
                "\"old\":" + oldVisitorStats.getDurPerSv() + "}," +
                "{\"type\":\"平均访问页面数(人次)\"," +
                "\"new\":" + newVisitorStats.getPvPerSv() + "," +
                "\"old\":" + oldVisitorStats.getPvPerSv()
                + "}]}}";
        return JSON.parseObject(json);
    }

    @RequestMapping("/hr")
    public Object getMidStatsGroupbyHourNewFlag(@RequestParam(value = "date",defaultValue = "0") Integer date ) {
        if(date==0)  date=now();
        List<VisitorStats> visitorStatsHrList = surgarService.getVisitorStatsByHour(date);

        //构建24位数组
        VisitorStats[] visitorStatsArr=new VisitorStats[24];

        //把对应小时的位置赋值
        for (VisitorStats visitorStats : visitorStatsHrList) {
            visitorStatsArr[visitorStats.getHr()] =visitorStats ;
        }
        List<String> hrList=new ArrayList<>();
        List<Long> uvList=new ArrayList<>();
        List<Long> pvList=new ArrayList<>();
        List<Long> newMidList=new ArrayList<>();

        //循环出固定的0-23个小时  从结果map中查询对应的值
        for (int hr = 0; hr <=23 ; hr++) {
            VisitorStats visitorStats = visitorStatsArr[hr];
            if (visitorStats!=null){
                uvList.add(visitorStats.getUv_ct())   ;
                pvList.add( visitorStats.getPv_ct());
                newMidList.add( visitorStats.getNew_uv());
            }else{ //该小时没有流量补零
                uvList.add(0L)   ;
                pvList.add( 0L);
                newMidList.add( 0L);
            }
            //小时数不足两位补零
            hrList.add(String.format("%02d", hr));
        }
        //拼接字符串
        String json = "{\"status\":0,\"data\":{" + "\"categories\":" +
                "[\""+StringUtils.join(hrList,"\",\"")+ "\"],\"series\":[" +
                "{\"name\":\"uv\",\"data\":["+ StringUtils.join(uvList,",") +"]}," +
                "{\"name\":\"pv\",\"data\":["+ StringUtils.join(pvList,",") +"]}," +
                "{\"name\":\"新用户\",\"data\":["+ StringUtils.join(newMidList,",") +"]}]}}";
        return  JSON.parseObject(json);
    }

    @RequestMapping("/keyword")
    public Object getKeywordStats(@RequestParam(value = "date",defaultValue = "0") Integer date,
                                  @RequestParam(value = "limit",defaultValue = "20") int limit){
        if(date==0){
            date=now();
        }
        //查询数据
        List<KeywordStats> keywordStatsList = surgarService.getKeywordStats(date, limit);
        StringBuilder jsonSb=new StringBuilder( "{\"status\":0,\"msg\":\"\",\"data\":[" );
        //循环拼接字符串
        for (int i = 0; i < keywordStatsList.size(); i++) {
            KeywordStats keywordStats =  keywordStatsList.get(i);
            if(i>=1){
                jsonSb.append(",");
            }
            jsonSb.append(  "{\"name\":\"" + keywordStats.getKeyword() + "\"," +
                    "\"value\":"+keywordStats.getCt()+"}");
        }
        jsonSb.append(  "]}");
        return JSON.parseObject(jsonSb.toString());
    }

}
