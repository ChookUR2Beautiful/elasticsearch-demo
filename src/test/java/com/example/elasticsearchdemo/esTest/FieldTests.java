package com.example.elasticsearchdemo.esTest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.elasticsearchdemo.po.PutWordResult;
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
 * 字段测试
 * Created by jianjian on 2019/1/31.
 */
public class FieldTests {

    String INDEX = "/field-tests";

    String MAPPING = "/events_stored";

    ElasticsearchService elasticsearchService = new ElasticsearchServiceImpl();

    /**
     * 创建索引和映射
     */
    @Before
     public void createIndex(){
        try {
            elasticsearchService.createIndex(INDEX);
            //两个字段,一个store一个没sotre
            String body= "{\"events_stored\":{\"properties\":{\"name\":{\"type\":\"text\",\"store\":true},\"noStoreName\":{\"type\":\"text\",\"store\":false}}}}";
            elasticsearchService.createMapping(INDEX,MAPPING,body);
            String word = "{\"name\":\"gouwa\",\"notStoreName\":\"gousheng\"}";
            elasticsearchService.putWord(INDEX,MAPPING,word,"1");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取source字段
     */
    @Test
    public void getSource() throws Exception {
        HttpClientResult httpClientResult = HttpClientUtils.doGet(ES_URL + INDEX + MAPPING + "/1");
        System.out.println(httpClientResult);
        JSONObject jsonObject = JSON.parseObject(httpClientResult.getContent()).getJSONObject("_source");
        //包括所有字段 2个
        Assert.assertTrue(2==jsonObject.size());
    }

    /**
     * 只查询store字段
     */
    @Test
    public void getStoredFields() throws Exception {
        HttpClientResult httpClientResult = HttpClientUtils.doGet(ES_URL + INDEX + MAPPING + "/1?stored_fields=name");
        System.out.println(httpClientResult);
        JSONObject jsonObject = JSON.parseObject(httpClientResult.getContent()).getJSONObject("fields");
        Assert.assertNotNull(jsonObject.get("name"));
    }


    /**
     * 自动生成ID
     */
    @Test
    public void putWordReturnIdTest(){
        String word = "{\"name\":\"gouwa\",\"notStoreName\":\"gousheng\"}";
        PutWordResult putWordResult = elasticsearchService.postWord(INDEX, MAPPING, word, "");
        System.out.println(putWordResult);
        Assert.assertNotNull(putWordResult.get_id());
    }

    /**
     * 使用API更新文档 更改部分字段
     */
    @Test
    public void updateWordTest() throws Exception {
        String doc = "{\"doc\":{\"name\":\"goudan\"}}";
        HttpClientResult httpClientResult = HttpClientUtils.doPost(ES_URL + INDEX + MAPPING + "/1/_update", null, null, doc);
        System.out.println(httpClientResult);

        HttpClientResult result = HttpClientUtils.doGet(ES_URL + INDEX + MAPPING + "/1");
        System.out.println(result);
        String name = JSON.parseObject(result.getContent()).getJSONObject("_source").getString("name");
        Assert.assertEquals(name,"goudan");

    }

    /**
     * 使用upsert来创建不存在的文档
     */
    @Test
    public void upsertTest() throws Exception {
        String body = "{\"doc\":{\"name\":\"goudan\"},\"upsert\":{\"name\":\"gougou\"}}";
        String id = new Random().nextInt(10000)+"";
        HttpClientResult httpClientResult = HttpClientUtils.doPost(ES_URL + INDEX + MAPPING + "/"+id+"/_update", null, null, body);
        System.out.println(httpClientResult);

        HttpClientResult result = HttpClientUtils.doGet(ES_URL + INDEX + MAPPING + "/"+id);
        System.out.println(result);
        String name = JSON.parseObject(result.getContent()).getJSONObject("_source").getString("name");
        Assert.assertEquals(name,"gougou");
    }


    /**
     * 索引文档时使用版本号
     */
    @Test
    public void putWordByVersionTest() throws Exception {
        String id = new Random().nextInt(100000)+"";
        String word = "{\"name\":\"gouwa\",\"notStoreName\":\"gousheng\"}";
        for(int i=0;i<10;i++){
            //10次根据version索引文档
            HttpClientResult result = HttpClientUtils.doPut(ES_URL + INDEX + MAPPING + "/" + id + (i > 0 ? ("?version=" + i) : ""), null, word);
            System.out.println(result);
            Integer version = JSON.parseObject(result.getContent()).getInteger("_version");
            Assert.assertTrue(version==i+1);
        }
    }


    /**
     * 删除文档
     */
    @Test
    public void deleteTheWord() throws Exception {
        HttpClientResult result = HttpClientUtils.doDelete(ES_URL + INDEX + MAPPING + "/2");

        HttpClientResult getReturn = HttpClientUtils.doGet(ES_URL + INDEX + MAPPING + "/2");
        Boolean found = JSON.parseObject(getReturn.getContent()).getBoolean("found");
        Assert.assertFalse(found);
    }





}
