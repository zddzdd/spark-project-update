package com.micro.bigdata.dao;

import com.micro.bigdata.domain.LoginCount;

/**
 * 登录次数DAO接口
 */
public interface ILoginCountDAO {
    void insert(LoginCount loginCount);
}
