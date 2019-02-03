package com.example.elasticsearchdemo.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.elasticsearchdemo.po.PutWordResult;
import com.example.elasticsearchdemo.service.ElasticsearchService;
import com.example.elasticsearchdemo.util.HttpClientUtils;
import com.example.elasticsearchdemo.util.pojo.HttpClientResult;

import static com.example.elasticsearchdemo.constant.Constant.ES_URL;

/**
 * Created by jianjian on 2019/1/31.
 */
public class ElasticsearchServiceImpl implements ElasticsearchService{
    @Override
    public boolean createIndex(String index) {
        HttpClientResult httpClientResult = null;
        try {
            httpClientResult = HttpClientUtils.doPut(ES_URL  + index, null, null);
            JSONObject jsonObject = analyzeHttpResult(httpClientResult);
            return jsonObject.getBoolean("acknowledged");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean createMapping(String index, String mapping, String body) {
        HttpClientResult httpClientResult = null;
        try {
            httpClientResult = HttpClientUtils.doPut(ES_URL +index+ "/_mapping"+mapping + "?pretty",null,body);
            JSONObject jsonObject = analyzeHttpResult(httpClientResult);
            //创建成功
            return jsonObject.getBoolean("acknowledged");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public PutWordResult putWord(String index, String mapping, String word, String id) {
        try {
            HttpClientResult httpClientResult = HttpClientUtils.doPut(ES_URL + index + mapping + "/" + id, null, word);
            JSONObject jsonObject = analyzeHttpResult(httpClientResult);
            return jsonObject.toJavaObject(PutWordResult.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public PutWordResult postWord(String index, String mapping, String word, String id) {
        try {
            HttpClientResult httpClientResult = HttpClientUtils.doPost(ES_URL + index + mapping + "/" + id, null,null, word);
            JSONObject jsonObject = analyzeHttpResult(httpClientResult);
            return jsonObject.toJavaObject(PutWordResult.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private JSONObject analyzeHttpResult(HttpClientResult httpClientResult) {
        if(200!=httpClientResult.getCode()&&201!=httpClientResult.getCode()){
            System.out.println(httpClientResult.getContent());
            throw new RuntimeException("http请求失败,状态码为:"+httpClientResult.getCode());
        }
        JSONObject jsonObject = JSON.parseObject(httpClientResult.getContent());
        return jsonObject;
    }
}
