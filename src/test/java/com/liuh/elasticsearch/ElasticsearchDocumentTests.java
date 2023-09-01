package com.liuh.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.UpdateResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.liuh.elasticsearch.entity.Hotel;
import com.liuh.elasticsearch.mapper.HotelMapper;
import com.liuh.elasticsearch.vo.HotelVo;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: liuhuan
 * @Date: 2023/8/14 15:41
 * @PackageName: com.liuh.elasticsearch
 * @ClassName: ElasticsearchDocumentTests
 * @Description: TODO
 * @Version 1.0
 */
@SpringBootTest
public class ElasticsearchDocumentTests {
    private RestClient restClient;
    private ElasticsearchTransport transport;
    private ElasticsearchClient client;

    private final String INDEX_NAME = "hotel";
    @Autowired
    private HotelMapper hotelMapper;

    /**
     * 在执行操作前建立连接
     */
    @BeforeEach
    void setUp() {
        this.restClient = RestClient.builder(new HttpHost("123.249.106.95", 9200)).build();
        this.transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        this.client = new ElasticsearchClient(transport);
    }

    /**
     * 操作完成后关闭连接
     *
     * @throws IOException
     */
    @AfterEach
    void setDown() throws IOException {
        this.transport.close();
        this.restClient.close();
    }

    /**
     * 添加文档，如果索引未创建，也会创建索引
     *
     * @throws IOException
     */
    @Test
    void createIndexAndAddDoc() throws IOException {
        Hotel hotel = hotelMapper.selectById(1);
        HotelVo hotelVo = new HotelVo(hotel);
        IndexResponse response = client.index(i -> i
                .index(INDEX_NAME)
                .id(hotelVo.getId().toString())
                .document(hotelVo)
        );
        System.out.println(response.index());
    }

    /**
     * 根据 id 获取文档信息
     *
     * @throws IOException
     */
    @Test
    void getDocById() throws IOException {
        // 需要对应的类有无参构造函数
        GetResponse<HotelVo> response = client.get(g -> g.index(INDEX_NAME).id("1"), HotelVo.class);
        System.out.println(response.source());
    }

    /**
     * 根据 id 修改文档
     *
     * @throws IOException
     */
    @Test
    void updateDicById() throws IOException {
        // 构建要修改的内容
        Map<String, Object> map = new HashMap<>(16);
        map.put("city", "长沙");
        UpdateResponse<HotelVo> response = client.update(u -> u.index(INDEX_NAME).id("1").doc(map), HotelVo.class);
    }

    /**
     * 根据 id 删除文档
     *
     * @throws IOException
     */
    @Test
    void deleteDocById() throws IOException {
        client.delete(d -> d.index(INDEX_NAME).id("1"));
    }

    /**
     * 批量操作 新增文档
     */
    @Test
    void addDoc() throws IOException {
        List<Hotel> hotels = hotelMapper.selectList(null);
        BulkRequest.Builder br = new BulkRequest.Builder();
        for (Hotel hotel : hotels) {
            HotelVo hotelVo = new HotelVo(hotel);
            br.operations(op -> op.index(idx -> idx.index(INDEX_NAME)
                    .id(hotelVo.getId().toString())
                    .document(hotelVo)));
        }
        client.bulk(br.build());
    }
}
