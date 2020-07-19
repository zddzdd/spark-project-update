package com.micro.bigdata.dao;

import com.micro.bigdata.domain.AdStat;

import java.util.List;

public interface IAdStatDAO {
    void updateBatch(List<AdStat> adStats);
}
