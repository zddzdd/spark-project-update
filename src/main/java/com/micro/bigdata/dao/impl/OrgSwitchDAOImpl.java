package com.micro.bigdata.dao.impl;

import com.micro.bigdata.dao.IOrgSwitchDAO;
import com.micro.bigdata.domain.OrgSwitch;
import com.micro.bigdata.jdbc.JDBCHelper;

public class OrgSwitchDAOImpl implements IOrgSwitchDAO {
    @Override
    public void insert(OrgSwitch orgSwitch) {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        String insert_sql = "INSERT INTO org_exchange_log(user_id,org_id,exchange_time) value(?,?,?)";
        Object[]param = new Object[]{orgSwitch.getUser_id(),orgSwitch.getOrg_id(),orgSwitch.getSwitch_time()};
        jdbcHelper.executeUpdate(insert_sql,param);
    }
}
