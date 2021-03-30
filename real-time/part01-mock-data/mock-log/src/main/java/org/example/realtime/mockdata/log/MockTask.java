package org.example.realtime.mockdata.log;

import org.example.realtime.mockdata.log.config.AppConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;


@Component
public class MockTask {


    @Autowired
    ThreadPoolTaskExecutor poolExecutor;

    public void mainTask() {

        for (int i = 0; i < AppConfig.mock_count; i++) {
            //poolExecutor.execute(new Mocker());
            System.out.println("active+" + poolExecutor.getActiveCount());
            new org.example.realtime.mockdata.log.Mocker().run();
        }
    }
}
