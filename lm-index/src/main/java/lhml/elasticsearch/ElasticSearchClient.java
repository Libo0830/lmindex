package lhml.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * @FileName: ElasticSearchClient
 * @author: bli
 * @date: 2020年02月20日 16:32
 * @description:
 */
public class ElasticSearchClient {
    /**
     * 高阶Rest Client
     */
    private static RestHighLevelClient client = null;
    /**
     * 低阶Rest Client
     */
    private static RestClientBuilder restClient = null;

//    @Value("${smart.construction.sendNotice}")
//    private static String hostUrl;

    /**
     * 使用饿汉模式创建RestHighLevelClient
     */
    public static RestHighLevelClient getRestHighLevelClient(){
        if (client == null) {
            synchronized (RestHighLevelClient.class) {
                if (client == null) {
                    client = getClient();
                }
            }
        }
        return client;
    }

    /**
     * 获取高阶client
     *
     * 集群模式  RestClient.builder中 创建多个HttpHost 以逗号隔开
     * @return
     */
    private static RestHighLevelClient getClient() {
        RestHighLevelClient client = null;
        try {
            HttpHost host1 = new HttpHost("10.72.16.125", 9200, "http");
//            HttpHost host2 = new HttpHost("10.72.16.125", 9200, "http");
//            RestClientBuilder rClient = RestClient.builder(host1, host2);
            RestClientBuilder rClient = RestClient.builder(host1);
            client = new RestHighLevelClient(rClient);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return client;
    }

    /**
     * 获取低阶client
     * @return
     */
    private static RestClient getRestClient() {
        RestClient client = null;

        try {
            HttpHost host1 = new HttpHost("10.72.16.125", 9200, "http");
//            HttpHost host2 = new HttpHost("10.72.16.125", 9200, "http");
//            RestClientBuilder rClient = RestClient.builder(host1, host2);
            RestClientBuilder rClient = RestClient.builder(host1);
            client = rClient.build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return client;
    }

    /**
     * 高级客户端将在内部创建用于根据提供的构建器执行请求的低级客户端。
     * 这个低级客户端维护一个连接池，并启动一些线程，
     * 因此当您真正完成高级客户端工作时，应该关闭它，
     * 而它将反过来关闭内部低级客户端来释放这些资源。
     * 这可以通过关闭来完成
     */
    public static void closeClient() {
        try {
            if (client != null) {
                client.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
