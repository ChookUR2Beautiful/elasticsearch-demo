package com.example.elasticsearchdemo.esTest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.elasticsearchdemo.service.ElasticsearchService;
import com.example.elasticsearchdemo.service.impl.ElasticsearchServiceImpl;
import com.example.elasticsearchdemo.util.HttpClientUtils;
import com.example.elasticsearchdemo.util.pojo.HttpClientResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static com.example.elasticsearchdemo.constant.Constant.ES_URL;

/**
 * 映射测试集
 * Created by jianjian on 2019/1/31.
 */
public class MappingTests {

    String INDEX = "/mapping-tests";

    String NEW_INDEX = "/mapping-tests2"+ new Random().nextInt(10000);

    String MAPPINGS = "/new-events";

    ElasticsearchService elasticsearchService = new ElasticsearchServiceImpl();

    /**
     * 添加一份文档,自动创建映射
     * @throws Exception
     */
    @Before
    public void indexOneWord() throws Exception {
        String body="{\"name\":\"Late Night with Elasticsearch\",\"date\":\"2013-10-25T19:00\"}";
        HttpClientUtils.doPut(ES_URL+INDEX+MAPPINGS+"/1",null,body);
    }


    /**
     * 直接获取映射
     */
    @Test
    public void getMappingTest() throws Exception {
        HttpClientResult httpClientResult = HttpClientUtils.doGet(ES_URL +INDEX+ "/_mapping"+MAPPINGS + "?pretty");
        System.out.println(httpClientResult.getContent());
        JSONObject jsonObject = JSON.parseObject(httpClientResult.getContent())
                .getJSONObject("mapping-tests")
                .getJSONObject("mappings")
                .getJSONObject("new-events")
                .getJSONObject("properties");
        //date字段为date类型
        Assert.assertEquals("date",jsonObject.getJSONObject("date").getString("type"));
        //name字段为text类型
        Assert.assertEquals("text",jsonObject.getJSONObject("name").getString("type"));
    }


    /**
     * 定义新的映射
     * @throws Exception
     */
    @Test
    public void createNewMappingTest() throws Exception {
        elasticsearchService.createIndex(NEW_INDEX);
        String body="{\"new-events\":{\"properties\":{\"host\":{\"type\":\"text\"}}}}";
        boolean acknowledged = elasticsearchService.createMapping(NEW_INDEX, MAPPINGS, body);
        //创建成功
        Assert.assertTrue(acknowledged);
    }

    /**
     * 扩展一个新映射
     * @throws Exception
     */
    @Test
    public void extendNewMapping() throws Exception {
        String body="{\"new-events\":{\"properties\":{\"host\":{\"type\":\"text\"}}}}";
        HttpClientResult httpClientResult = HttpClientUtils.doPut(ES_URL +INDEX+ "/_mapping"+MAPPINGS + "?pretty",null,body);
        System.out.println(httpClientResult.getContent());
        JSONObject jsonObject = JSON.parseObject(httpClientResult.getContent());
        //创建成功
        Assert.assertTrue(jsonObject.getBoolean("acknowledged"));
    }

    /**
     * 无法改版现有的字段的数据类型
     * @throws Exception
     */
    @Test
    public void extendSameMapping() throws Exception {
        String body="{\"new-events\":{\"properties\":{\"host\":{\"type\":\"long\"}}}}";
        HttpClientResult httpClientResult = HttpClientUtils.doPut(ES_URL +INDEX+ "/_mapping"+MAPPINGS + "?pretty",null,body);
        System.out.println(httpClientResult.getContent());
        JSONObject jsonObject = JSON.parseObject(httpClientResult.getContent());
        //创建失败,报400
        Assert.assertEquals(new Integer(400),jsonObject.getInteger("status"));
    }


}
