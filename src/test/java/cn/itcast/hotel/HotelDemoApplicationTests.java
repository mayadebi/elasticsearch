package cn.itcast.hotel;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@SpringBootTest
class HotelDemoApplicationTests {
    private RestHighLevelClient client;
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
    @Test
    void contextLoads() {
        System.out.println("aaaa");
    }

}
