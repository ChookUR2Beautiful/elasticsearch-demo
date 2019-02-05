package com.example.elasticsearchdemo.esTest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.elasticsearchdemo.po.PutWordResult;
import com.example.elasticsearchdemo.service.ElasticsearchService;
import com.example.elasticsearchdemo.service.impl.ElasticsearchServiceImpl;
import com.example.elasticsearchdemo.util.HttpClientUtils;
import com.example.elasticsearchdemo.util.pojo.HttpClientResult;
import org.apache.http.client.HttpClient;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.Map;
import java.util.Random;

import static com.example.elasticsearchdemo.constant.Constant.ES_URL;

/**
 * Created by jianjian on 2019/1/25.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CreateIndexTests {

    ElasticsearchService elasticsearchService = new ElasticsearchServiceImpl();

    /**
     * ping通ES服务器
     * @throws Exception
     */
    @Test
    public void A_pingTest() throws Exception {
        HttpClientResult httpClientResult = HttpClientUtils.doGet(ES_URL);
        String content = httpClientResult.getContent();
        System.out.println(content);
        Assert.assertNotNull(content);
    }

    /**
     * 创建一个文档
     */
    @Test
    public void B_putWord() throws Exception {
        String id= new Random().nextInt(100000)+"";
        //group是类型,创建文档的时候会自动创建类型,也可以手动创建
        String url=ES_URL+"/get-together/group/"+id+"?pretty";
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name","Elasticsearch Denver");
        jsonObject.put("organizer","Lee");
        jsonObject.put("sort",10086L);
        PutWordResult putWordResult = elasticsearchService.putWord("/get-together", "/group", jsonObject.toJSONString(), id);
        Assert.assertEquals(putWordResult.get_id(),id);
    }

    /**
     * 手动创建索引
     */
    @Test
    public void C_putIndex() throws Exception {
        String id= new Random().nextInt(10000)+"";
        String index="/new-index"+id;
        boolean result = elasticsearchService.createIndex(index);
        Assert.assertTrue(result );
    }

    /**
     * 获取映射
     * @throws Exception
     */
    @Test
    public void D_getMapping() throws Exception {
        HttpClientResult httpClientResult = HttpClientUtils.doGet(ES_URL + "/get-together/_mapping/group?pretty");
        JSONObject jsonObject = JSON.parseObject(httpClientResult.getContent());
        String string = jsonObject
                .getJSONObject("get-together")
                .getJSONObject("mappings")
                .getJSONObject("group")
                .getJSONObject("properties")
                .getJSONObject("name")
                .getString("type");
        //name字段类型为text
        Assert.assertEquals(string,"text");
    }

}
