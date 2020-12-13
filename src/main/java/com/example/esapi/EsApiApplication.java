package com.example.esapi;

import com.example.esapi.util.ElasticsearchUtil;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

@SpringBootApplication
public class EsApiApplication {

    public  static final String INDEX_NAME = "jd_goods";
    public static void main(String[] args) {
        SpringApplication.run(EsApiApplication.class, args);
    }

    @Resource
    private RestHighLevelClient client;

    @PostConstruct
    public void initIndex(){
        ElasticsearchUtil util = new ElasticsearchUtil(client);
        util.createIndex(INDEX_NAME);
    }

}
