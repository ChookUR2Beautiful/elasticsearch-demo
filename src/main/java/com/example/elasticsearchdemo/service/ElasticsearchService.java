package com.example.elasticsearchdemo.service;

import com.example.elasticsearchdemo.po.PutWordResult;

/**
 * Created by jianjian on 2019/1/31.
 */
public interface ElasticsearchService {

    /**
     * 创建新的索引
     * @param index
     * @return
     */
    boolean createIndex(String index);

    /**
     * 创建新的映射,前台是索引已存在
     * @param index
     * @param mapping
     * @param body
     * @return
     */
    boolean createMapping(String index, String mapping, String body);

    /**
     * 插入文档
     *  @param index
     * @param mapping
     * @param word
     * @param i
     */
    PutWordResult putWord(String index, String mapping, String word, String id);

    /**
     * post方式插入文档
     * @param index
     * @param mapping
     * @param word
     * @param s
     * @return
     */
    PutWordResult postWord(String index, String mapping, String word, String s);
}
