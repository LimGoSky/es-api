package com.example.esapi;

import com.alibaba.fastjson.JSON;
import com.example.esapi.pojo.User;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * 高级客户端测试 7.10.0
 */
@SpringBootTest
class EsApiApplicationTests {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    //测试索引的创建 Request
    @Test
    void testCreateIndex() throws IOException {
        //1、创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("lim_index");
        //2、执行
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);

    }

    //测试索引是否存在
    @Test
    void testExitIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("lim_index");
        boolean exists = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    //测试删除索引
    @Test
    void testDeleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("lim_index");
        AcknowledgedResponse delete = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }

    //创建文档
    @Test
    void testAddDocument() throws IOException {
        //创建对象
        User user = new User("test", 8);
        IndexRequest request = new IndexRequest("lim_index");

        //规则 put/lim_index/_doc/1
        request.id("1");
        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");

        //数据放入请求
        request.source(JSON.toJSONString(user), XContentType.JSON);

        //客户端发送请求，获取相应结果
        IndexResponse index = restHighLevelClient.index(request, RequestOptions.DEFAULT);
        System.out.println(index.toString());
    }

    //获取文档
    @Test
    void testIsExits() throws IOException {
        GetRequest request = new GetRequest("lim_index", "1");
        //不获取返回的_source的上下文
        request.fetchSourceContext(new FetchSourceContext(false));
        request.storedFields("_none_");

        boolean exists = restHighLevelClient.exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    //获取文档信息
    @Test
    void testGetDocument() throws IOException {
        GetRequest request = new GetRequest("lim_index", "1");
        GetResponse documentFields = restHighLevelClient.get(request, RequestOptions.DEFAULT);
        //打印文档信息
        System.out.println(documentFields.getSourceAsString());
        System.out.println(documentFields);
    }

    //更新档信息
    @Test
    void testUpdateDocument() throws IOException {
        UpdateRequest request = new UpdateRequest("lim_index", "1");
        request.timeout("1S");

        User user = new User("1234", 20);
        request.doc(JSON.toJSONString(user), XContentType.JSON);
        UpdateResponse update = restHighLevelClient.update(request, RequestOptions.DEFAULT);
        System.out.println(update.status());

    }

    //更新档信息
    @Test
    void testDeleteDocument() throws IOException {
        DeleteRequest request = new DeleteRequest("lim_index", "3");
        request.timeout("1S");

        DeleteResponse delete = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.status());

    }

    //特殊的，批量插入
    @Test
    void testBulkRequest() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");

        ArrayList<User> userList = new ArrayList();
        userList.add(new User("111",18));
        userList.add(new User("222",18));
        userList.add(new User("333",18));
        userList.add(new User("444",18));
        userList.add(new User("555",18));

        //批处理
        for (int i = 0; i < userList.size(); i++) {
            //批量更新和删除，在这里操作
            bulkRequest.add(
                    new IndexRequest("lim_index")
                            .id("" +(i+1))
                            .source(JSON.toJSONString(userList.get(i)),XContentType.JSON));
        }
        BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulk.hasFailures());//是否失败
    }

    //查询
    @Test
    void testSearch() throws IOException {
        SearchRequest request = new SearchRequest("lim_index");
        //构建搜索条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //条件
        //QueryBuilders.termQuery精确匹配
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "111");
        //QueryBuilders.matchAllQuery();//匹配所有
        searchSourceBuilder.query(termQueryBuilder);
        //searchSourceBuilder.from();
        //searchSourceBuilder.sort();
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        request.source(searchSourceBuilder);

        SearchResponse search = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(search.getHits()));
        System.out.println("======================================");
        for (SearchHit hit : search.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
    }

}
