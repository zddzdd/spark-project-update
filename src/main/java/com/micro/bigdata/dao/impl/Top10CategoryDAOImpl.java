package com.micro.bigdata.dao.impl;

import com.micro.bigdata.dao.ITop10CategoryDAO;
import com.micro.bigdata.domain.Top10Category;
import com.micro.bigdata.jdbc.JDBCHelper;

public class Top10CategoryDAOImpl implements ITop10CategoryDAO {
    @Override
    public void insert(Top10Category category) {
        String sql = "insert into top10_category values(?,?,?,?,?)";
        Object[] params = new Object[]{category.getTaskid(),
        category.getCategoryid(),
        category.getClickCount(),
        category.getOrderCount(),
        category.getPayCount()};
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql,params);
    }
}
