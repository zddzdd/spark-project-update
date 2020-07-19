package com.micro.bigdata.dao;

import com.micro.bigdata.domain.AreaTop3Product;

import java.util.List;

/**
 * 各区域top3热门商品DAO接口
 */
public interface IAreaTop3ProductDAO {
    void insertBatch(List<AreaTop3Product> areaTop3Products);
}
