package com.micro.bigdata.dao;

import com.micro.bigdata.domain.SessionRandomExtract;

public interface ISessionRandomExtractDAO {
    /**
     * 插入session随机抽取
     * @param sessionRandomExtract
     */
    void insert(SessionRandomExtract sessionRandomExtract);
}
