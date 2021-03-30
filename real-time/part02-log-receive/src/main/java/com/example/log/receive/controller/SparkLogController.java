package com.example.log.receive.controller;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j // 借助slf4j；落盘
public class SparkLogController {

    //    private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(LogController.class);

    @Autowired
    KafkaTemplate kafkaTemplate;


    @RequestMapping(path = "/applog1")
    public String receiveAppLog(@RequestBody String applog) {
        log.info(applog);

        JSONObject json = JSONObject.parseObject(applog);
        if (json.containsKey("start")) {
            kafkaTemplate.send("rt_start_log", applog);
        } else {
            kafkaTemplate.send("rt_event_log", applog);
        }
        return "success";
    }
}
