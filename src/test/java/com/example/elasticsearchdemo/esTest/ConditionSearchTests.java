package com.example.elasticsearchdemo.esTest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.example.elasticsearchdemo.util.HttpClientUtils;
import com.example.elasticsearchdemo.util.pojo.HttpClientResult;
import org.junit.Assert;
import org.junit.Test;

import static com.example.elasticsearchdemo.constant.Constant.ES_URL;

/**
 * 条件搜索
 * Created by jianjian on 2019/2/1.
 */
public class ConditionSearchTests {

    /**
     * 搜索全部
     */
    @Test
    public void searchAllTest() throws Exception {
        HttpClientResult result = HttpClientUtils.doGet(ES_URL + "/_search");
        System.out.println(result);
        Integer total = JSON.parseObject(result.getContent()).getJSONObject("hits").getInteger("total");
        //结果超过一个
        Assert.assertTrue(total>0);
    }


    /**
     * 搜索整个索引
     */
    @Test
    public void searchIndexAllTest() throws Exception {
        HttpClientResult result = HttpClientUtils.doGet(ES_URL + "/get-together/_search");
        System.out.println(result);
        Integer total = JSON.parseObject(result.getContent()).getJSONObject("hits").getInteger("total");
        //结果超过一个
        Assert.assertTrue(total>0);
    }




    /**
     * 搜索索引类型
     */
    @Test
    public void searchMappingAllTest() throws Exception {
        HttpClientResult result = HttpClientUtils.doGet(ES_URL + "/get-together/group/_search");
        System.out.println(result);
        Integer total = JSON.parseObject(result.getContent()).getJSONObject("hits").getInteger("total");
        //结果超过一个
        Assert.assertTrue(total>0);
    }


    /**
     * 搜索索引类型
     */
    @Test
    public void searchMappingAllTest1() throws Exception {
        HttpClientResult result = HttpClientUtils.doGet(ES_URL + "/_all/group/_search");
        System.out.println(result);
        Integer total = JSON.parseObject(result.getContent()).getJSONObject("hits").getInteger("total");
        //结果超过一个
        Assert.assertTrue(total>0);
    }

    /**
     * 搜索索引类型
     */
    @Test
    public void searchMappingAllTest2() throws Exception {
        HttpClientResult result = HttpClientUtils.doGet(ES_URL + "/*/group/_search");
        System.out.println(result);
        Integer total = JSON.parseObject(result.getContent()).getJSONObject("hits").getInteger("total");
        //结果超过一个
        Assert.assertTrue(total>0);
    }


    /**
     * 多个索引和类型一起搜索
     */
    @Test
    public void searchMoreIndexMappingTest() throws Exception {
        HttpClientResult result = HttpClientUtils.doGet(ES_URL + "/get-together,mapping-tests/group,new-events/_search");
        System.out.println(result);
        Integer total = JSON.parseObject(result.getContent()).getJSONObject("hits").getInteger("total");
        //结果超过一个
        Assert.assertTrue(total>0);
    }

    /**
     * 搜索名字以get-toge开头的索引,但是不包括get-together
     * @throws Exception
     */
    @Test
    public void searchThanIndexTest() throws Exception {
        HttpClientResult result = HttpClientUtils.doGet(ES_URL + "/get-toge*,-get-together/group/_search");
        System.out.println(result);
        Integer total = JSON.parseObject(result.getContent()).getJSONObject("hits").getInteger("total");
        Assert.assertTrue(total==0);
    }


    /**
     * 分页搜索
     */
    @Test
    public void searchPageTest() throws Exception {
        HttpClientResult result = HttpClientUtils.doGet(ES_URL + "/get-together/_search?from=10&size=10");
        System.out.println(result);
        Integer size = JSON.parseObject(result.getContent()).getJSONObject("hits").getJSONArray("hits").size();
        Assert.assertTrue(size<=10);
    }


    /**
     * 排序搜索
     */
    @Test
    public void searchSortTest() throws Exception {
        //按sort字段倒序
        HttpClientResult result = HttpClientUtils.doGet(ES_URL + "/get-together/_search?sort=sort:desc");
        System.out.println(result);
        JSONArray jsonArray = JSON.parseObject(result.getContent()).getJSONObject("hits").getJSONArray("hits");
        Long sort = jsonArray.getJSONObject(0).getJSONObject("_source").getLong("sort");
        //第一条数据的sort值大于0
        Assert.assertTrue(sort>0);
        //搜索结果条数大于0
        Assert.assertTrue(jsonArray.size()>0);
    }


    /**
     * 筛选部分字段
     */
    @Test
    public void searchBySourceTest() throws Exception {
        HttpClientResult result = HttpClientUtils.doGet(ES_URL + "/get-together/_search?_source=name,sort");
        System.out.println(result);
        JSONArray jsonArray = JSON.parseObject(result.getContent()).getJSONObject("hits").getJSONArray("hits");
        for(int i=0;i<jsonArray.size();i++) {
            JSONObject o = jsonArray.getJSONObject(i);
            JSONObject source = o.getJSONObject("_source");
            //name和sort字段至少存在一个
            Assert.assertTrue(source.get("name") != null || source.get("sort") != null);
        }
    }


    /**
     * 搜索某字段的匹配的内容
     */
    @Test
    public void searchByContent() throws Exception {
        //搜索字段为name并包含elasticsearch的内容
        HttpClientResult result = HttpClientUtils.doGet(ES_URL + "/get-together/_search?q=name:elasticsearch");
        System.out.println(result);
        JSONArray jsonArray = JSON.parseObject(result.getContent()).getJSONObject("hits").getJSONArray("hits");
        for(int i=0;i<jsonArray.size();i++) {
            JSONObject o = jsonArray.getJSONObject(i);
            JSONObject source = o.getJSONObject("_source");
            String name = source.getString("name").toLowerCase();
            //转化小写的内容包含elasticsearch
            Assert.assertTrue(name.indexOf("elasticsearch")>=0 );
        }

    }

    /**
     * post方式搜索
    /*{
        "query": {
        "match_all": {}
    },
        "from": 0,   //从低0条开始
        "size": 10,  //查询10条
        "_source": ["name", "sort"],   //指定字段
        "sort": [{
            "sort": "desc"      //sort字段倒序
        }]
    }*/
    @Test
    public void postMoethodSearchTest() throws Exception {
        String body = "{\"query\":{\"match_all\":{}},\"from\":0,\"size\":10,\"_source\":[\"name\",\"sort\"],\"sort\":[{\"sort\":\"desc\"}]}";
        HttpClientResult result = HttpClientUtils.doPost(ES_URL + "/get-together/_search",null,null,body);
        System.out.println(result);
        Integer size = JSON.parseObject(result.getContent()).getJSONObject("hits").getJSONArray("hits").size();
        Assert.assertTrue(size<=10);
        JSONArray jsonArray = JSON.parseObject(result.getContent()).getJSONObject("hits").getJSONArray("hits");
        Long sort = jsonArray.getJSONObject(0).getJSONObject("_source").getLong("sort");
        //第一条数据的sort值大于0
        Assert.assertTrue(sort>0);
        //搜索结果条数大于0
        Assert.assertTrue(jsonArray.size()>0);
        for(int i=0;i<jsonArray.size();i++) {
            JSONObject o = jsonArray.getJSONObject(i);
            JSONObject source = o.getJSONObject("_source");
            //name和sort字段至少存在一个
            Assert.assertTrue(source.get("name") != null || source.get("sort") != null);
        }
        for(int i=0;i<jsonArray.size();i++) {
            JSONObject o = jsonArray.getJSONObject(i);
            JSONObject source = o.getJSONObject("_source");
            String name = source.getString("name").toLowerCase();
            //转化小写的内容包含elasticsearch
            Assert.assertTrue(name.indexOf("elasticsearch")>=0 );
        }
    }



}
