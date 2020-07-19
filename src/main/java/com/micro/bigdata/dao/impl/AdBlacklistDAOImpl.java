package com.micro.bigdata.dao.impl;

import com.micro.bigdata.dao.IAdBlacklistDAO;
import com.micro.bigdata.domain.AdBlacklist;
import com.micro.bigdata.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class AdBlacklistDAOImpl implements IAdBlacklistDAO {
    @Override
    public void insertBatch(List<AdBlacklist> adBlacklists) {
        String sql = "INSERT INTO ad_blacklist VALUES(?)";

        List<Object[]> paramsList = new ArrayList<Object[]>();

        for(AdBlacklist adBlacklist : adBlacklists) {
            Object[] params = new Object[]{adBlacklist.getUserid()};
            paramsList.add(params);
        }

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeBatch(sql, paramsList);
    }

    @Override
    public List<AdBlacklist> findAll() {
        String sql = "SELECT * FROM ad_blacklist";

        final List<AdBlacklist> adBlacklists = new ArrayList<AdBlacklist>();

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        jdbcHelper.executeQuery(sql, null, new JDBCHelper.QueryCallback() {

            @Override
            public void process(ResultSet rs) throws Exception {
                while(rs.next()) {
                    long userid = Long.valueOf(String.valueOf(rs.getInt(1)));

                    AdBlacklist adBlacklist = new AdBlacklist();
                    adBlacklist.setUserid(userid);

                    adBlacklists.add(adBlacklist);
                }
            }

        });

        return adBlacklists;
    }
}
