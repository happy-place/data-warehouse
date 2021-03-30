package org.example.realtime.mockdata.db;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
@MapperScan("org.example.realtime.mockdata.db.mapper")
public class Gmall2020MockDbApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(Gmall2020MockDbApplication.class, args);

        MockTask mockTask = context.getBean(MockTask.class);

        mockTask.mainTask();
    }
}
