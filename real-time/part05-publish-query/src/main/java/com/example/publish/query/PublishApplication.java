package com.example.publish.query;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableAutoConfiguration
public class PublishApplication {

    public static void main(String[] args) {
        SpringApplication.run(PublishApplication.class, args);
    }

}
