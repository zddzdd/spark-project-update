package com.micro.bigdata.dao;

import com.micro.bigdata.domain.OrgSwitch;

/**
 * 用户切换日志的DAO接口
 */
public interface IOrgSwitchDAO {
    void insert(OrgSwitch orgSwitch);
}
