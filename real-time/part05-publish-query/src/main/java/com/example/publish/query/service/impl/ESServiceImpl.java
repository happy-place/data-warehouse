package com.example.publish.query.service.impl;

import com.example.publish.query.service.ESService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ESServiceImpl implements ESService {

    @Autowired
    private JestClient jedisClient;

    @Override
    public Long getDauTotal(String date) {
        String index = "gmall"+date.substring(0,4)+"_dau_info_"+date;
        String query = new SearchSourceBuilder().query(new MatchAllQueryBuilder()).toString();
        Search search = new Search.Builder(query).addIndex(index).addType("_doc").build();
        Long total = null;
        try {
            SearchResult result = jedisClient.execute(search);
            if(result.getTotal()!=null){
                total = result.getTotal();
            }
            return total;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, Long> getDauHour(String date) {
        String index = "gmall"+date.substring(0,4)+"_dau_info_"+date;
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        String aggName = "groupby_hour";
        TermsAggregationBuilder aggBuilder = AggregationBuilders.terms(aggName).field("hr").size(24);
        searchSourceBuilder.aggregation(aggBuilder);
        System.out.println(searchSourceBuilder.toString());
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(index).addType("_doc").build();
        Map<String,Long> hourDauMap = new HashMap<>();
        try {
            SearchResult result = jedisClient.execute(search);
            TermsAggregation termsAggregation = result.getAggregations().getTermsAggregation(aggName);
            if(null!=termsAggregation){
                List<TermsAggregation.Entry> buckets = result.getAggregations().getTermsAggregation(aggName).getBuckets();
                if(buckets!=null){
                    for(TermsAggregation.Entry bucket:buckets){
                        hourDauMap.put(bucket.getKey(),bucket.getCount());
                    }
                }
            }
            return hourDauMap;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
