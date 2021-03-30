package com.example.log.receive.controller;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j // 借助slf4j；落盘
public class FlinkLogController {

    //    private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(LogController.class);

    @Autowired
    KafkaTemplate kafkaTemplate;

    @RequestMapping(path = "/applog2")
    public String receiveAppLog(@RequestParam("param") String applog) {
        log.info(applog);
        System.out.println(applog);
        kafkaTemplate.send("ods_base_log",applog);
        return "success";
    }
}
