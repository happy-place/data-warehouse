package com.example.publish.query.service;

import java.util.Map;

public interface ESService {

    Long getDauTotal(String date);

    Map<String,Long> getDauHour(String date);

}
