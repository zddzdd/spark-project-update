package com.micro.bigdata.dao.impl;

import com.micro.bigdata.dao.ILoginCountDAO;
import com.micro.bigdata.domain.LoginCount;
import com.micro.bigdata.jdbc.JDBCHelper;

public class LoginCountDAOImpl implements ILoginCountDAO {
    @Override
    public void insert(LoginCount loginCount) {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        Object[]param = new Object[]{
                loginCount.getDate(),
                loginCount.getOrg_id(),
                loginCount.getPc_code(),
                loginCount.getUser_count()
        };
        String sql = "insert into user_login_info(login_date,org_id,platform_code,user_count) value(????)";
        jdbcHelper.executeUpdate(sql,param);
    }
}
