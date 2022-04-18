package com.jiuha.elasticsearchapidemo.utils;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.elasticsearch.indices.GetIndexResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.util.ObjectBuilder;
import com.jiuha.elasticsearchapidemo.entity.Store;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Elasticsearch 8 RestClient 工具类
 */
public class EsRestUtils {
    private static RestClient restClient;

    private static ElasticsearchClient client;

    private static ElasticsearchTransport transport;

    static {
        restClient = RestClient
                .builder(new HttpHost("192.168.12.170", 9200, "http"))
                .build();

        // 使用Jackson映射器创建传输层
        transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper()
        );
        client = new ElasticsearchClient(transport);
    }

    /**
     * 初始化RestClient和EsClient
     */
    public static void init() {
        restClient = RestClient
                .builder(new HttpHost("192.168.12.170", 9200, "http"))
                .build();

        // 使用Jackson映射器创建传输层
        transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper()
        );
        client = new ElasticsearchClient(transport);
    }

    /**
     * 关闭连接
     */
    public static void close() {
        try {
            if (Objects.nonNull(restClient)) {
                restClient.close();
            }
            if (Objects.nonNull(transport)) {
                transport.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static ElasticsearchClient getClient() {
        return client;
    }

    /**
     * 创建索引
     *
     * @param index 索引
     * @return 结果
     */
    public static Boolean create(String index) throws IOException {
        CreateIndexResponse response = client.indices().create(c -> c.index(index));
        return response.acknowledged();
    }

    /**
     * 查询索引
     *
     * @param index 索引
     * @return 结果
     */
    public static GetIndexResponse query(String index) throws IOException {
        return client.indices().get(c -> c.index(index));
    }

    /**
     * 删除索引
     *
     * @param index 所以给
     * @return 删除结果
     * @throws IOException IO异常
     */
    public static Boolean delete(String index) throws IOException {
        DeleteIndexResponse response = client.indices().delete(e -> e.index(index));
        return response.acknowledged();
    }

    /**
     * 添加文档
     *
     * @param index 添加索引
     * @param id    ID
     * @param obj   文档信息
     * @return 响应
     * @throws IOException IO异常
     */
    public static Result addDocument(String index, String id, Object obj) throws IOException {
        CreateResponse response = client
                .create(e -> e.index(index).id(id).document(obj));
        return response.result();
    }

    /**
     * 查询文档
     *
     * @param index 索引
     * @param id    id
     * @param clazz JSON格式类
     * @param <T>   转换目标类型
     * @return 查询结果
     * @throws IOException IO异常
     */
    public static <T> GetResponse<T> queryDocument(String index, String id, Class<T> clazz) throws IOException {
        return client.get(e -> e.index(index).id(id), clazz);
    }

    /**
     * 修改文档
     *
     * @param index 索引
     * @param id    主键
     * @param obj   修改内容
     * @param clazz 类
     * @param <T>   实体类型
     * @return 修改结果
     * @throws IOException IO异常
     */
    public static <T> UpdateResponse<T> modifyDocument(String index, String id, Object obj, Class<T> clazz) throws IOException {
        return client.update(e -> e.index(index).id(id).doc(obj), clazz);
    }

    /**
     * 删除文档
     *
     * @param index 索引
     * @param id    主键ID
     * @return 删除结果返回
     * @throws IOException IO异常
     */
    public static DeleteResponse removeDocument(String index, String id) throws IOException {
        return client.delete(e -> e.index(index).id(id));
    }

    /**
     * 批量添加文档
     *
     * @param index 索引
     * @param list  添加列表
     * @return 批量结果
     * @throws IOException IO异常
     */
    public static <T> BulkResponse batchAddDocument(String index, List<T> list) throws IOException {
        List<BulkOperation> opList = list.stream().map(e -> new BulkOperation.Builder().create(d -> d.document(e).index(index)).build()).toList();
        return client.bulk(e -> e.index(index).operations(opList));
    }

    /**
     * 批量删除
     *
     * @param index 索引
     * @param ids   删除主键IDS列表
     * @return 批量操作结果
     * @throws IOException IO异常
     */
    public static BulkResponse batchDeleteDocument(String index, List<String> ids) throws IOException {
        List<BulkOperation> opList = ids.stream()
                .map(e -> new BulkOperation.Builder().delete(d -> d.id(e).index(index)).build())
                .toList();
        return client.bulk(e -> e.index(index).operations(opList));
    }

    /**
     * 分页查询
     * @param index 查询索引
     * @param queryCondition 查询条件
     * @param from 开始页号
     * @param size 分页大小
     * @param clazz 目标类型
     * @param <T> 泛型
     * @return 查询结果
     * @throws IOException IO异常
     */
    public static <T> SearchResponse<T> pagingQueryDocument(String index, Query queryCondition, Integer from, Integer size, Class<T> clazz) throws IOException {
        //返回结果在hits中
        return client.search(s -> s.index(index)
                        .query(queryCondition)
                        .from(from)
                        .size(size)
                , clazz);
    }

    public static void main(String[] args) {
        try {
//            EsRestUtils.create("store");
//            Store entity = new Store();
//            entity.setName("测试店铺");
//            entity.setCity("南京");
//            entity.setLocation("118.777689,32.048787");
//            EsRestUtils.addDocument("store", "1", entity);
//            GetIndexResponse response = EsRestUtils.query("store");
            GetResponse<Store> response = EsRestUtils.queryDocument("store", "1", Store.class);
            System.out.println(response.source());
            EsRestUtils.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
