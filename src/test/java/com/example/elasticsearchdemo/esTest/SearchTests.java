package com.example.elasticsearchdemo.esTest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.elasticsearchdemo.util.HttpClientUtils;
import com.example.elasticsearchdemo.util.pojo.HttpClientResult;
import org.junit.Assert;
import org.junit.Test;

import static com.example.elasticsearchdemo.constant.Constant.ES_URL;

/**
 * Created by jianjian on 2019/1/28.
 */
public class SearchTests {
    /**
     * URI搜索获取数据
     */
    @Test
    public void A_searchTest() throws Exception {
        // q=搜索内容 size=查询条数
        HttpClientResult httpClientResult = HttpClientUtils.doGet(ES_URL + "/get-together/group/_search?q=elasticsearch&size=10&pretty");
        System.out.println(httpClientResult.getContent());
        JSONObject jsonObject = JSON.parseObject(httpClientResult.getContent());
        Integer total = jsonObject.getJSONObject("hits").getInteger("total");
        Assert.assertTrue(total>=1);
    }

    /**
     * json方式搜索
     */
    @Test
    public void B_postSearchTest() throws Exception {
        //{
        //	"query":{
        //		"query_string":{
        //			"query":"elasticsearch"
        //		}
        //	}
        //}
        String body="{\"query\":{\"query_string\":{\"query\":\"elasticsearch\"}}}";
        HttpClientResult httpClientResult = HttpClientUtils.doPost(ES_URL + "/get-together/group/_search",null, null, body);
        System.out.println(httpClientResult.getContent());
        JSONObject jsonObject = JSON.parseObject(httpClientResult.getContent());
        Integer total = jsonObject.getJSONObject("hits").getInteger("total");
        Assert.assertTrue(total>=1);
    }


    /**
     * json方式搜索
     *
     */
    @Test
    public void C_postSearchTest2() throws Exception {
      /*  {
        "query":{
            "query_string":{
                "query":"elasticsearch san francisco", //搜索内容
                        "default_field":"name",    //指定字段
                        "default_operator":"AND"   //条件and,必须符合所有字符
            }
        }
    */
        String body="{\"query\":{\"query_string\":{\"query\":\"elasticsearch san francisco\",\"default_field\":\"name\",\"default_operator\":\"AND\"}}}";
        HttpClientResult httpClientResult = HttpClientUtils.doPost(ES_URL + "/get-together/group/_search",null, null, body);
        System.out.println(httpClientResult.getContent());
        JSONObject jsonObject = JSON.parseObject(httpClientResult.getContent());
        Integer total = jsonObject.getJSONObject("hits").getInteger("total");
        Assert.assertTrue(total==0);
    }


    /**
     * json方式搜索
     *
     */
    @Test
    public void D_postSearchTest3() throws Exception {
      /*  {
	"query":{
		"term":{ //直观的根据字段名搜索
			"name":"elasticsearch" //name是字段名,内容必须全部完全匹配
		}
	}
}
    */
        String body="{\"query\":{\"term\":{\"name\":\"elasticsearch\"}}}";
        HttpClientResult httpClientResult = HttpClientUtils.doPost(ES_URL + "/get-together/group/_search",null, null, body);
        System.out.println(httpClientResult.getContent());
        JSONObject jsonObject = JSON.parseObject(httpClientResult.getContent());
        Integer total = jsonObject.getJSONObject("hits").getInteger("total");
        Assert.assertTrue(total>=1);

        String body1="{\"query\":{\"term\":{\"name\":\"elasticsearch san francisco\"}}}";
        HttpClientResult httpClientResult1 = HttpClientUtils.doPost(ES_URL + "/get-together/group/_search",null, null, body1);
        System.out.println(httpClientResult1.getContent());
        JSONObject jsonObject1 = JSON.parseObject(httpClientResult1.getContent());
        Integer total1 = jsonObject1.getJSONObject("hits").getInteger("total");
        Assert.assertTrue(total1==0);
    }


    /**
     * 根据id获取
     */
    @Test
    public void E_getByIdTest() throws Exception {
        String id="1";
        HttpClientResult httpClientResult = HttpClientUtils.doGet(ES_URL + "/get-together/group/"+id+"?pretty");
        System.out.println(httpClientResult.getContent());
        JSONObject jsonObject = JSON.parseObject(httpClientResult.getContent());
        String _id = jsonObject.getString("_id");
        Assert.assertEquals(id,_id);
    }


}
