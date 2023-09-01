package com.liuh.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.CompletionSuggest;
import co.elastic.clients.elasticsearch.core.search.CompletionSuggestOption;
import co.elastic.clients.elasticsearch.core.search.CompletionSuggester;
import co.elastic.clients.elasticsearch.core.search.Suggestion;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.util.ObjectBuilder;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @Author: liuhuan
 * @Date: 2023/8/19 18:25
 * @PackageName: com.liuh.elasticsearch
 * @ClassName: SuggestTests
 * @Description: TODO
 * @Version 1.0
 */
@SpringBootTest
public class SuggestTests {
    private final Function<CompletionSuggester.Builder, ObjectBuilder<CompletionSuggester>> builderObjectBuilderFunction = c -> c;
    private RestClient restClient;
    private ElasticsearchTransport transport;
    private ElasticsearchClient client;

    private final String INDEX_NAME = "test2";


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
     * 自动补全测试
     *
     * @throws IOException
     */
    @Test
    void suggest() throws IOException {
        // 查询
        SearchResponse<Object> response = client.search(s -> s.index(INDEX_NAME)
                        .suggest(suggest -> suggest.suggesters("title_suggest", su -> su.text("s").completion(
                                c -> c.field("title").skipDuplicates(Boolean.TRUE).size(10)
                        ))),
                Object.class);
        // 结果解析
        Map<String, List<Suggestion<Object>>> suggest = response.suggest();
        for (Map.Entry<String, List<Suggestion<Object>>> entry : suggest.entrySet()) {
            List<Suggestion<Object>> value = entry.getValue();
            for (Suggestion<Object> suggestion : value) {
                CompletionSuggest<Object> completion = suggestion.completion();
                List<CompletionSuggestOption<Object>> options = completion.options();
                for (CompletionSuggestOption<Object> option : options) {
                    System.out.println(option);
                }
            }
        }
    }
}
