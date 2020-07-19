package com.micro.bigdata.dao;

import com.micro.bigdata.domain.Top10Session;

/**
 * top10活跃session的DAO接口
 */
public interface ITop10SessionDAO {
    void insert(Top10Session top10Session);
}
