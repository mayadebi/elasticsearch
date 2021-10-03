package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import cn.itcast.hotel.service.impl.HotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

import static cn.itcast.hotel.esJson.Hotel.ES_JSON;

@SpringBootTest
class HotelDemoApplicationTests {
    private RestHighLevelClient client;
    @Autowired
    private IHotelService hotelService;
    //  开始前注入
    @BeforeEach
    void setUp(){
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://106.52.4.161:9200")
        ));
    }
    // 结束后关闭
    @AfterEach
    void xiaohui() throws IOException {
        this.client.close();
    }
    // 创建索引库
    @Test
    void createSuoyin() throws IOException {
        // 创建reques对象
        CreateIndexRequest request = new CreateIndexRequest("hotel");
        // 使用dsl语句
        request.source(ES_JSON, XContentType.JSON);
        // 发送请求
        client.indices().create(request, RequestOptions.DEFAULT);
    }
    // 删除索引库
    @Test
    void deleteSuoyin() throws IOException {
        // 创建reques对象
        DeleteIndexRequest request = new DeleteIndexRequest("hotel");
        // 发送请求
        client.indices().delete(request,RequestOptions.DEFAULT);
    }
    // 判断索引库是否存在
    @Test
    void extstsSuoyin() throws IOException {
        // 创建reques对象
        GetIndexRequest request = new GetIndexRequest("hotel");
        // 发送请求
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    // 新增文档
    @Test
    void createWendang() throws IOException {
        // 查询
        Hotel byId = hotelService.getById(36934);
        // 转换为文档类型，主要是经纬度
        HotelDoc hotelDoc = new HotelDoc(byId);
        IndexRequest request = new IndexRequest("hotel").id(hotelDoc.getId().toString());
        // 对象转json
        request.source(JSON.toJSONString(hotelDoc),XContentType.JSON);
        client.index(request,RequestOptions.DEFAULT);
    }
    // 查询文档
    @Test
    void getWendang() throws IOException {
        GetRequest request = new GetRequest("hotel","36934");
        GetResponse resp = client.get(request, RequestOptions.DEFAULT);
        String json = resp.getSourceAsString();
        System.out.println(json);
        HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
    }
    // 局部更新，全量更新和新增没有区别
    @Test
    void uptWendang() throws IOException {
        UpdateRequest request = new UpdateRequest("hotel","36934");
        request.doc(
                "price","952"
        );
        client.update(request, RequestOptions.DEFAULT);
    }
    // 删除文档
    @Test
    void delWendang() throws IOException {
        DeleteRequest request = new DeleteRequest("hotel","36934");
        client.delete(request,RequestOptions.DEFAULT);
    }
    // 批量新增
    @Test
    void piliangWendang() throws IOException {
        BulkRequest request = new BulkRequest();
        List<Hotel> list = hotelService.list();
        for (Hotel hotel : list) {
            HotelDoc hotelDoc = new HotelDoc(hotel);
            request.add(new IndexRequest("hotel").id(hotelDoc.getId().toString())
                    .source(JSON.toJSONString(hotelDoc),XContentType.JSON));
        }
        client.bulk(request,RequestOptions.DEFAULT);
    }
}
