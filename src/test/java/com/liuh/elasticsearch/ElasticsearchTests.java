package com.liuh.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.DistanceUnit;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.FunctionBoostMode;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.liuh.elasticsearch.vo.HotelVo;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Author: liuhuan
 * @Date: 2023/8/16 19:46
 * @PackageName: com.liuh.elasticsearch
 * @ClassName: ElasticsearchTests
 * @Description: TODO
 * @Version 1.0
 */
@SpringBootTest
public class ElasticsearchTests {

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
     * match_all
     * 查询所有
     *
     * @throws IOException
     */
    @Test
    void matchAll() throws IOException {
        // 查询
        SearchResponse<HotelVo> response = client.search(s -> s.index(INDEX_NAME)
                .query(q -> q.matchAll(m -> m)), HotelVo.class);
        // 结果数据解析
        responseResult(response);
    }

    /**
     * match_all
     * 查询所有
     *
     * @throws IOException
     */
    @Test
    void matchAll2() throws IOException {
        // 查询
        SearchRequest.Builder builder = new SearchRequest.Builder();
        SearchRequest request = builder.build();
        SearchResponse<HotelVo> response = client.search(request, HotelVo.class);
        // 结果数据解析
        responseResult(response);
    }

    /**
     * match_query
     * 根据一个字段查询，查询名称包含 汉庭 的酒店查询所有
     *
     * @throws IOException
     */
    @Test
    void match() throws IOException {
        // 查询
        SearchResponse<HotelVo> response = client.search(s -> s.index(INDEX_NAME)
                .query(q -> q.match(m -> m.field("name").query("汉庭"))), HotelVo.class);
        // 结果数据解析
        responseResult(response);
    }

    /**
     * 根据多个字段查询，查询名称或城市包含 长沙 的酒店 multi_match_query
     *
     * @throws IOException
     */
    @Test
    void multiMatch() throws IOException {
        ArrayList<String> keys = new ArrayList<>();
        keys.add("name");
        keys.add("city");
        // 查询
        SearchResponse<HotelVo> response = client.search(s -> s.index(INDEX_NAME)
                .query(q -> q.multiMatch(m -> m.fields(keys).query("长沙"))), HotelVo.class);
        // 结果数据解析
        responseResult(response);
    }

    /**
     * term
     * 根据词条精确值查询，查看在 岳麓区 的酒店
     *
     * @throws IOException
     */
    @Test
    void term() throws IOException {
        // 查询
        SearchResponse<HotelVo> response = client.search(s -> s.index(INDEX_NAME)
                        .query(q -> q.term(t -> t.field("address").value("岳麓区"))),
                HotelVo.class);
        // 结果数据解析
        responseResult(response);
    }

    /**
     * range
     * 根据值的范围查询，价格区间在 200 - 500 的酒店
     *
     * @throws IOException
     */
    @Test
    void range() throws IOException {
        // 查询
        SearchResponse<HotelVo> response = client.search(s -> s.index(INDEX_NAME)
                        .query(q -> q.range(r -> r.field("price").gte(JsonData.of(200)).lte(JsonData.of(500)))),
                HotelVo.class);
        // 结果数据解析
        responseResult(response);
    }

    /**
     * geo_distance
     * 查询到指定中心点小于某个距离值的所有文档，以 40, 40 为中心，查询 12km 内的所有酒店
     *
     * @throws IOException
     */
    @Test
    void geoDistance() throws IOException {
        // 查询
        SearchResponse<HotelVo> response = client.search(s -> s.index(INDEX_NAME)
                        .query(q -> q.geoDistance(g -> g.field("location").distance("12km")
                                .location(l -> l.text("40,40")))),
                HotelVo.class);
        // 结果数据解析
        responseResult(response);
    }

    /**
     * geo_bounding_box
     * 查询 geo_point 值落在某个矩形范围的所有文档，查询纬度在 50.01-22.37 之间、经度在 51.12-22.1 之间的酒店
     *
     * @throws IOException
     */
    @Test
    void geoBoundingBox() throws IOException {
        // 查询
        SearchResponse<HotelVo> response = client.search(s -> s.index(INDEX_NAME)
                        .query(q -> q.geoBoundingBox(g -> g.field("location").
                                boundingBox(b -> b.tlbr(t -> t.topLeft(tl -> tl.text("50.01, 51.12"))
                                        .bottomRight(br -> br.text("22.73, 22.1")))))),
                HotelVo.class);
        // 结果数据解析
        responseResult(response);
    }

    /**
     * 将 id 为 1 的文档分数设置为 42
     *
     * @throws IOException
     */
    @Test
    void functionScore() throws IOException {
        // 查询
        SearchResponse<HotelVo> response = client.search(s -> s.index(INDEX_NAME)
                        .query(q -> q.functionScore(f -> f.functions(fs -> fs
                                        .filter(filter -> filter.match(m -> m.field("id").query("1")))
                                        .weight(42d))
                                .boostMode(FunctionBoostMode.Multiply))),
                HotelVo.class);
        // 结果数据解析
        responseResult(response);
    }

    /**
     * bool 复合查询
     * 获取在长沙的所有酒店，芙蓉区的优先
     *
     * @throws IOException
     */
    @Test
    void bool() throws IOException {
        // 查询
        SearchResponse<HotelVo> response = client.search(s -> s.index(INDEX_NAME)
                        .query(q -> q.bool(b -> b.must(m -> m.matchAll(ma -> ma))
                                .should(should -> should.match(match -> match.field("address").query("芙蓉区")))
                                .filter(filter -> filter.term(t -> t.field("city").value("长沙"))))),
                HotelVo.class);
        // 结果数据解析
        responseResult(response);
    }

    /**
     * sort 普通排序
     * 先按照价格从低到高排序，然后按照打分从高到底排
     *
     * @throws IOException
     */
    @Test
    void ordinarySort() throws IOException {
        // 查询
        SearchResponse<HotelVo> response = client.search(s -> s.index(INDEX_NAME)
                        .query(q -> q.matchAll(m -> m))
                        .sort(sort -> sort.field(f -> f.field("price").order(SortOrder.Asc)))
                        .sort(sort -> sort.field(f -> f.field("score").order(SortOrder.Desc))),
                HotelVo.class);
        // 结果数据解析
        responseResult(response);
    }


    /**
     * sort 地理坐标排序
     * 获取距离经纬坐标（22, 33）距离最近的酒店，结果按照单位为 km 显示
     *
     * @throws IOException
     */
    @Test
    void geoDistanceSort() throws IOException {
        // 查询
        SearchResponse<HotelVo> response = client.search(s -> s.index(INDEX_NAME)
                        .query(q -> q.matchAll(m -> m))
                        .sort(sort -> sort.geoDistance(g -> g.field("location").
                                location(l -> l.text("22, 33")).
                                order(SortOrder.Asc).unit(DistanceUnit.Kilometers))),
                HotelVo.class);
        // 结果数据解析
        responseResult(response);
    }

    /**
     * 分页
     * 从下标索引 1 开始，返回两条数据
     *
     * @throws IOException
     */
    @Test
    void paging() throws IOException {
        // 查询
        SearchResponse<HotelVo> response = client.search(s -> s.index(INDEX_NAME)
                        .query(q -> q.matchAll(m -> m)).from(1).size(2),
                HotelVo.class);
        // 结果数据解析
        responseResult(response);
    }

    /**
     * 高亮查询
     * 查询文档中包含 ”长沙“ 的数据，将 city 字段的 "长沙" 高亮显示
     *
     * @throws IOException
     */
    @Test
    void searchHighlight() throws IOException {
        // 查询
        SearchResponse<HotelVo> response = client.search(s -> s.index(INDEX_NAME)
                        .query(q -> q.match(m -> m.field("all").query("长沙")))
                        .highlight(h -> h.fields("city",
                                f -> f.preTags("<liu>").postTags("</liu>").requireFieldMatch(false))),
                HotelVo.class);
        // 结果数据解析
        responseResult(response);

    }

    /**
     * 请求结果解析
     *
     * @param response
     */
    private void responseResult(SearchResponse<HotelVo> response) {
        System.out.println(response);
        HitsMetadata<HotelVo> hits = response.hits();
        List<Hit<HotelVo>> hitList = hits.hits();
        long value = hits.total().value();
        System.out.println("文档总数：" + value);
        for (Hit<HotelVo> hotelVoHit : hitList) {
            HotelVo hotelVo = hotelVoHit.source();
            Double score = hotelVoHit.score();
            Map<String, List<String>> highlight = hotelVoHit.highlight();
            Set<Map.Entry<String, List<String>>> entries = highlight.entrySet();
            for (Map.Entry<String, List<String>> entry : entries) {
                hotelVo.setCity(entry.getValue().get(0));
            }
            System.out.println(hotelVo);
            System.out.println(score);
        }
    }
}
