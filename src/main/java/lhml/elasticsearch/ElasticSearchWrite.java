package lhml.elasticsearch;

import lhml.utils.ClassNameConvertUtils;
import org.apache.commons.beanutils.BeanUtils;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

/**
 * @FileName: ElasticSearchWrite
 * @author: bli
 * @date: 2020年02月20日 16:44
 * @description:
 */
public class ElasticSearchWrite {


    private static RestHighLevelClient client = ElasticSearchClient.getRestHighLevelClient();

    /**
     * 单个新增
     * @param source    ES目标对象
     * @param cls       ES目标对象class
     */
    public static void createIndex(Object source, Class<?> cls) {
        //获取类名并将首字母转小写
        String className = ClassNameConvertUtils.lowerFirstChar(cls.getSimpleName());
        try {
            Map<String, Object> paramJsonMap = BeanUtils.describe(source);
            //去掉转换后map中的class  key
            paramJsonMap.remove("class");
            IndexRequest request = new IndexRequest(className, className,paramJsonMap.get("id").toString()).source(paramJsonMap);
            //同步执行
            IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
            String index = indexResponse.getIndex();
            String id = indexResponse.getId();
            DocWriteResponse.Result result = indexResponse.getResult();
            System.out.println("index:"+ index+",id:"+id+",result:"+result);
            if (result == DocWriteResponse.Result.CREATED) {
                //处理(如果需要)第一次创建文档的情况
                System.out.println("文件创建成功");
            } else if (result == DocWriteResponse.Result.UPDATED) {
                //处理(如果需要的话)当文档已经存在时被重写的情况
                System.out.println("文件更新成功");
            }
            ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                //处理成功碎片的数量少于总碎片的情况
                System.out.println("处理成功碎片的数量少于总碎片");
            }
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure :
                        shardInfo.getFailures()) {
                    //处理潜在的故障
                    String reason = failure.reason();
                    System.out.println("故障："+ reason);
                }
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 批量新增
     * @param list  数据集合
     * @param cls   放入ES的目标对象class
     * @throws IOException
     */
    public static void bulkCreateIndex(List list, Class<?> cls) throws IOException {
        //获取类名并将首字母转小写
        String className = ClassNameConvertUtils.lowerFirstChar(cls.getSimpleName());
        BulkRequest bulkRequest = new BulkRequest();
        if (list != null && !list.isEmpty()){
            list.forEach(item->{
                try {
                    Map<String, Object> paramJsonMap = BeanUtils.describe(item);
                    //去掉转换后map中的class  key
                    paramJsonMap.remove("class");
                    bulkRequest.add(new IndexRequest(className, className,paramJsonMap.get("id").toString()).source(paramJsonMap));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                } catch (NoSuchMethodException e) {
                    e.printStackTrace();
                }

            });
            //同步执行
            BulkResponse bulkResponse = client.bulk(bulkRequest);
            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                if (bulkItemResponse.isFailed()) {
                    BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                    continue;
                }
                DocWriteResponse itemResponse = bulkItemResponse.getResponse();
                if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.INDEX
                        || bulkItemResponse.getOpType() == DocWriteRequest.OpType.CREATE) {
                    IndexResponse indexResponse = (IndexResponse) itemResponse;
                } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.UPDATE) {
                    UpdateResponse updateResponse = (UpdateResponse) itemResponse;
                } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.DELETE) {
                    DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
                }
            }
        }

    }


}
