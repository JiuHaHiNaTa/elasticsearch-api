package com.jiuha.elasticsearchapidemo.utils;

import cn.hutool.core.text.CharSequenceUtil;
import cn.hutool.core.util.StrUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

/**
 * Elasticsearch 7.x REST API 工具类
 */
@Slf4j
public class Es7RestUtils {

    private static RestHighLevelClient client;

    static {
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.12.170", 9200, "http"),
                        new HttpHost("192.168.12.170", 9201, "http")
                )
        );
    }

    /**
     * 创建连接
     */
    public static RestHighLevelClient create() {
        if (client == null) {
            client = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost("192.168.12.170", 9200, "http"),
                            new HttpHost("192.168.12.170", 9201, "http")
                    )
            );
            return client;
        }
        return client;
    }

    /**
     * 关闭连接
     */
    public static void close() {
        try {
            client.close();
        } catch (IOException e) {
            log.info("close client error: {}", e.getMessage());
            e.printStackTrace();
        }
        client = null;
    }

    /**
     * 创建文档(同步)
     */
    public static IndexResponse post(String id, String index, String json) throws IOException {
        IndexRequest request = new IndexRequest(index);
        if (CharSequenceUtil.isNotBlank(id)) {
            request.id(id);
        }
        //以WriteRequest.RefreshPolicy实例形式设置刷新策略
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        //以字符串形式刷新策略
        request.setRefreshPolicy("wait_for");
        request.versionType(VersionType.EXTERNAL);
        request.source(json, XContentType.JSON);
        return client.index(request, RequestOptions.DEFAULT);
    }

    /**
     * 创建文档(异步)
     */
    public static void post(String id, String index, String json, org.elasticsearch.action.ActionListener<org.elasticsearch.action.index.IndexResponse> listener) {
        IndexRequest request = new IndexRequest(index);
        if (CharSequenceUtil.isNotBlank(id)) {
            request.id(id);
        }
        //以WriteRequest.RefreshPolicy实例形式设置刷新策略
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        //以字符串形式刷新策略
        request.setRefreshPolicy("wait_for");
        request.source(json, XContentType.JSON);
        client.indexAsync(request, RequestOptions.DEFAULT, listener);
    }

    /**
     * 获取文档
     */
    public static GetResponse get(String index, String id) throws IOException {
        return client.get(
                new GetRequest(index, id),
                RequestOptions.DEFAULT);
    }

    /**
     * 检查文档是否存在
     *
     * @param index 索引
     * @param id    主键ID
     * @return true:存在,false:不存在
     * @throws IOException IO异常
     */
    public static boolean exist(String index, String id) throws IOException {
        GetRequest getRequest = new GetRequest(
                index, //索引
                id);    //文档id
        //禁用fetching _source.
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        return client.exists(getRequest, RequestOptions.DEFAULT);
    }

    /**
     * 删除文档
     *
     * @param index 索引
     * @param id    文档ID
     * @return 删除响应结果
     * @throws IOException IO异常
     */
    public static DeleteResponse delete(String index, String id) throws IOException {
        DeleteRequest request = new DeleteRequest(index, id);
        return client.delete(request, RequestOptions.DEFAULT);
    }

    /**
     * 更新文档
     *
     * @param index 索引名
     * @param id    主键ID
     * @param json  文档内容JSON串
     * @return 响应结果
     * @throws IOException IO异常
     */
    public static UpdateResponse update(String index, String id, String json) throws IOException {
        UpdateRequest request = new UpdateRequest(index, id);
        request.doc(json, XContentType.JSON);
        return client.update(request, RequestOptions.DEFAULT);
    }


    public static void main(String[] args) throws IOException {
        Es7RestUtils.post("1", "/person", "{\"name\":\"张三\",\"age\":18}");
    }

}
