package com.liuh.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.elasticsearch.indices.GetIndexResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.io.StringReader;

@SpringBootTest
class ElasticsearchIndexTests {

    private RestClient restClient;
    private ElasticsearchTransport transport;
    private ElasticsearchClient client;
    private final String INDEX_NAME = "hotel";

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
     * 创建索引
     *
     * @throws IOException
     */
    @Test
    void creatHotelIndex() throws IOException {
        // 索引映射信息
        String mappings = "{\n" +
                "  \"mappings\": {\n" +
                "    \"properties\": {\n" +
                "      \"id\": {\n" +
                "        \"type\": \"keyword\"\n" +
                "      },\n" +
                "      \"name\": {\n" +
                "        \"type\": \"text\",\n" +
                "        \"analyzer\": \"ik_max_word\",\n" +
                "        \"copy_to\": \"all\"\n" +
                "      },\n" +
                "      \"address\": {\n" +
                "        \"type\": \"text\",\n" +
                "        \"analyzer\": \"ik_max_word\",\n" +
                "        \"copy_to\": \"all\"\n" +
                "      },\n" +
                "      \"price\": {\n" +
                "        \"type\": \"integer\",\n" +
                "        \"copy_to\": \"all\"\n" +
                "      },\n" +
                "      \"score\": {\n" +
                "        \"type\": \"integer\",\n" +
                "        \"copy_to\": \"all\"\n" +
                "      },\n" +
                "      \"brand\": {\n" +
                "        \"type\": \"keyword\",\n" +
                "        \"copy_to\": \"all\"\n" +
                "      },\n" +
                "      \"city\": {\n" +
                "        \"type\": \"keyword\",\n" +
                "        \"copy_to\": \"all\"\n" +
                "      },\n" +
                "      \"starName\": {\n" +
                "        \"type\": \"keyword\",\n" +
                "        \"copy_to\": \"all\"\n" +
                "      },\n" +
                "      \"business\": {\n" +
                "        \"type\": \"keyword\",\n" +
                "        \"copy_to\": \"all\"\n" +
                "      },\n" +
                "      \"location\": {\n" +
                "        \"type\": \"geo_point\"\n" +
                "      },\n" +
                "      \"pic\": {\n" +
                "        \"type\": \"text\",\n" +
                "        \"index\": false\n" +
                "      },\n" +
                "      \"all\": {\n" +
                "        \"type\": \"text\",\n" +
                "        \"analyzer\": \"ik_max_word\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";


        CreateIndexResponse response =
                client.indices().create(c -> c.index(INDEX_NAME)
                        .aliases("hotel-index", a -> a.isWriteIndex(true)).withJson(new StringReader(mappings)));
        System.out.println(response.acknowledged());
    }

    /**
     * 查看索引结构
     *
     * @throws IOException
     */
    @Test
    void getHotelIndex() throws IOException {
        GetIndexResponse response = client.indices().get(g -> g.index(INDEX_NAME));
        System.out.println(response);
    }

    /**
     * 删除索引
     *
     * @throws IOException
     */
    @Test
    void deleteHotelIndex() throws IOException {
        DeleteIndexResponse response = client.indices().delete(d -> d.index(INDEX_NAME));
        System.out.println(response.acknowledged());
    }


}
