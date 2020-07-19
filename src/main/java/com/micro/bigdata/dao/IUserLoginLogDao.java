package com.micro.bigdata.dao;

import com.micro.bigdata.domain.UserLoginLog;


/**
 * 用户登录日志的DAO接口
*/
public interface IUserLoginLogDao {
    void insert(UserLoginLog userLoginLogs);
}
