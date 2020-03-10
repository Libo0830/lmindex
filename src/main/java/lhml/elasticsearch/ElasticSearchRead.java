package lhml.elasticsearch;

import lhml.utils.ClassNameConvertUtils;
import net.sf.json.JSONArray;
import org.apache.commons.beanutils.BeanUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @FileName: ElasticSearchRead
 * @author: bli
 * @date: 2020年02月20日 16:43
 * @description:
 */
public class ElasticSearchRead {

    private static RestHighLevelClient client = ElasticSearchClient.getRestHighLevelClient();

    /**
     * 查询
     * @param queryColum        查询条件字段
     * @param param             查询条件值
     * @param pageIndex         分页页码
     * @param pageSize          分页
     * @param cls               查询ES目标对象class
     * @return
     * @throws IOException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static List search(String queryColum, String param, Integer pageIndex, Integer pageSize, Class<?> cls) throws IOException, InvocationTargetException, IllegalAccessException, InstantiationException {
        //获取类名并将首字母转小写
        String className = ClassNameConvertUtils.lowerFirstChar(cls.getSimpleName());
        //创建SearchRequest，其中构造参数为索引名称，type为类型名称，此处建议创建ES索引时，索引名称和类型名称相同。
        SearchRequest searchRequest = new SearchRequest(className).types(className);
        //使用默认选项创建一个SearchSourceBuilder。
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //设置查询。可以是任何类型的QueryBuilder，此处因为要使用分词器，需要使用match查询，分词器是和math匹配的。
        sourceBuilder.query(QueryBuilders.matchQuery(queryColum, param));
        //设置分页
        sourceBuilder.from(pageIndex*pageSize);
        sourceBuilder.size(pageSize);
        //设置一个可选的超时，控制允许搜索的时间。
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        //将SearchSourceBuilder添加到SearchRequest中
        searchRequest.source(sourceBuilder);
        //执行查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        //获取查询结果
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        List list = new ArrayList();
        for (SearchHit hit : searchHits) {
            Object obj = cls.newInstance();
            //转换查询结果，mapToBean
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            BeanUtils.populate(obj, sourceAsMap);
            System.out.println(sourceAsMap);
            list.add(obj);
        }
        JSONArray jsonArray = new JSONArray();
        jsonArray.addAll(list);
        System.out.println(jsonArray.toString());
        return list;
    }
}
