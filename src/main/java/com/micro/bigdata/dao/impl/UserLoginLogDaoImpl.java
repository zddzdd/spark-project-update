package com.micro.bigdata.dao.impl;

import com.micro.bigdata.dao.IUserLoginLogDao;
import com.micro.bigdata.domain.UserLoginLog;
import com.micro.bigdata.jdbc.JDBCHelper;

public class UserLoginLogDaoImpl implements IUserLoginLogDao {
    @Override
    public void insert(UserLoginLog userLoginLog) {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        String insertSQL = "INSERT INTO user_login_log(user_id,org_id,platform_code,client_type," +
                "login_type,app_version,phone,hw,nw,login_host,login_time) VALUE(?,?,?,?,?,?,?,?,?,?,?)";
        Object[]params = new Object[]{
                userLoginLog.getUser_id(),
                userLoginLog.getOrg_id(),
                userLoginLog.getPlatform_code(),
                userLoginLog.getClient_type(),
                userLoginLog.getLogin_type(),
                userLoginLog.getApp_version(),
                userLoginLog.getPhone(),
                userLoginLog.getHw(),
                userLoginLog.getNw(),
                userLoginLog.getLogin_host(),
                userLoginLog.getLogin_time()
        };
        jdbcHelper.executeUpdate(insertSQL,params);
    }

}
