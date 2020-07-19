package com.micro.bigdata.dao.impl;

import com.micro.bigdata.dao.ILoginDetailDAO;
import com.micro.bigdata.domain.LoginDetail;
import com.micro.bigdata.jdbc.JDBCHelper;

public class LoginDetailDAOImpl implements ILoginDetailDAO {
    @Override
    public void insert(LoginDetail loginDetail) {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        Object[]param = new Object[]{
                loginDetail.getDate(),
                loginDetail.getOrg_id(),
                loginDetail.getPc_code(),
                loginDetail.getUserid(),
                loginDetail.getLogin_time()
        };
        String insert_sql = "insert into user_login_detail(login_date,org_id,platform_code,user_id,last_login_time) value(?,?,?,?,?)";
        jdbcHelper.executeUpdate(insert_sql,param);
    }
}
