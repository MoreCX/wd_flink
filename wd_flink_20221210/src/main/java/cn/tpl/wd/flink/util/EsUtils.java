package cn.tpl.wd.flink.util;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author ChenXin
 * @create 2022-12-10-19:55
 */
public class EsUtils {
    public static RestHighLevelClient createEsClient(){
        Properties properties = PropertiesUtils.getProperties();
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(properties.getProperty("es.username"), properties.getProperty("es.password")));  //es账号密码

        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(properties.getProperty("es.hostname"), Integer.parseInt(properties.getProperty("es.port")), "http"))
                        .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                            @Override
                            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                                httpClientBuilder.disableAuthCaching();
                                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                            }
                        }));
    }

    /**
     * search after查询
     * @return
     * 返回值中key为 data的数据，value是 ArrayList<String>类型
     * 返回值中key为 sort_id的数据，value是 Object[] 类型
     * */
    public static ArrayList<String> searchAfterQueryData(SearchResponse search) throws IOException {
        ArrayList<String> data = new ArrayList<>();
        SearchHit[] hits = search.getHits().getHits();
        for (SearchHit hit : hits) {
            data.add(hit.getSourceAsString());
        }
        return data;
    }


    public static QueryBuilder booleanQuery(Map<String, Object> map) {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        map.forEach((key, value) -> {
            queryBuilder.filter(QueryBuilders.termQuery(key, value));
        });
        return queryBuilder;
    }


    public static ArrayList<String> query(RestHighLevelClient client, String indices, Map<String, Object> map) throws IOException {
        SearchRequest request = new SearchRequest().indices(indices);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(booleanQuery(map));
        request.source(sourceBuilder);
        return searchAfterQueryData(client.search(request, RequestOptions.DEFAULT));
    }




    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createEsClient();
        Map<String, Object> map = new HashMap<>();
        map.put("id.keyword", "0f22cbd55963db997dae4f2bdea5350d");
//        ArrayList<Map<String, Object>> response = queryFieldReturnData(client, "tpwd_dx_policy_prem_down", map);
        ArrayList<String> response = query(client, "tpwd_dx_policy_prem_trend", map);
        System.out.println((response));
        client.close();
    }

}
