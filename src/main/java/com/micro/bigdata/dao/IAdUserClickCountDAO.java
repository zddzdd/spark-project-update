package com.micro.bigdata.dao;

import com.micro.bigdata.domain.AdUserClickCount;

import java.util.List;

public interface IAdUserClickCountDAO {
    /**
     * 批量更新用户广告点击量
     * @param adUserClickCounts
     */
    void updateBatch(List<AdUserClickCount> adUserClickCounts);

    /**
     * 根据多个key查询用户广告点击量
     * @param date 日期
     * @param userid 用户id
     * @param adid 广告id
     * @return
     */
    int findClickCountByMultiKey(String date, long userid, long adid);
}
