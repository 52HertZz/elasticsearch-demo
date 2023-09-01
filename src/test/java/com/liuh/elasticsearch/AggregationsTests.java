package com.liuh.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.util.NamedValue;
import com.liuh.elasticsearch.vo.HotelVo;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: liuhuan
 * @Date: 2023/8/18 9:23
 * @PackageName: com.liuh.elasticsearch
 * @ClassName: AggregationsTests
 * @Description: TODO
 * @Version 1.0
 */
public class AggregationsTests {
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
     * 桶聚合
     * 根据城市分组，查询每个城市的价格小于等于 300 酒店数量，显示 2 条数据，按照文档数升序排序
     *
     * @throws IOException
     */
    @Test
    void bucket() throws IOException {
        // 设置排序信息
        List<NamedValue<SortOrder>> orderList = new ArrayList<>();
        NamedValue<SortOrder> value = new NamedValue<>("_count", SortOrder.Asc);
        orderList.add(value);
        // 查询
        SearchResponse<HotelVo> response = client.search(s -> s.index(INDEX_NAME)
                        .query(q -> q.range(r -> r.field("price").lte(JsonData.of(300))))
                        .size(0)
                        .aggregations("genres", a -> a.terms(t -> t.field("city")
                                .size(2)
                                .order(orderList))),
                HotelVo.class);

        // 结果打印
        System.out.println(response);
    }

    /**
     * 度量聚合
     * 获取每个城市的酒店的用户评分的 min、max、avg 等值，并按照平均值进行升序排序
     *
     * @throws IOException
     */
    @Test
    void metric() throws IOException {
        // 设置排序信息
        List<NamedValue<SortOrder>> orderList = new ArrayList<>();
        NamedValue<SortOrder> value = new NamedValue<>("stats_aggs.avg", SortOrder.Asc);
        orderList.add(value);
        // 查询
        SearchResponse<HotelVo> response = client.search(s -> s.index(INDEX_NAME)
                        .size(0)
                        .aggregations("genres", a -> a.terms(t -> t.field("city")
                                        .size(2)
                                        .order(orderList))
                                .aggregations("stats_aggs", aggs -> aggs.stats(st -> st.field("score")))),
                HotelVo.class);

        // 结果打印
        System.out.println(response);
        Aggregate genres = response.aggregations().get("genres");
        System.out.println(genres);
    }
}
