package com.micro.bigdata.dao.impl;

import com.micro.bigdata.dao.IAdProvinceTop3DAO;
import com.micro.bigdata.domain.AdProvinceTop3;
import com.micro.bigdata.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

public class AdProvinceTop3DAOImpl implements IAdProvinceTop3DAO {
    @Override
    public void updateBatch(List<AdProvinceTop3> adProvinceTop3s) {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        // 先做一次去重
        List<String> dateProvinces = new ArrayList<>();
        for(AdProvinceTop3 adProvinceTop3:adProvinceTop3s){
            String date = adProvinceTop3.getDate();
            String province = adProvinceTop3.getProvince();
            String key = date + "_" + province;

            if(!dateProvinces.contains(key)) {
                dateProvinces.add(key);
            }
        }
        // 根据去重后的date和province，进行批量删除操作
        String deleteSQL = "DELETE FROM ad_province_top3 WHERE date=? AND province=?";

        List<Object[]> deleteParamsList = new ArrayList<Object[]>();

        for(String dateProvince : dateProvinces) {
            String[] dateProvinceSplited = dateProvince.split("_");
            String date = dateProvinceSplited[0];
            String province = dateProvinceSplited[1];

            Object[] params = new Object[]{date, province};
            deleteParamsList.add(params);
        }
        try {
            jdbcHelper.executeBatch(deleteSQL, deleteParamsList);
        }catch (Exception e){
            System.out.println("deleteParamsList:"+deleteParamsList.toString());
            e.printStackTrace();
        }

        // 批量插入传入进来的所有数据
        String insertSQL = "INSERT INTO ad_province_top3 VALUES(?,?,?,?)";
        List<Object[]> insertParamsList = new ArrayList<Object[]>();
        for(AdProvinceTop3 adProvinceTop3:adProvinceTop3s){
            Object[]params = new Object[]{
                    adProvinceTop3.getDate(),
                    adProvinceTop3.getProvince(),
                    adProvinceTop3.getAdid(),
                    adProvinceTop3.getClickCount()};
            insertParamsList.add(params);
        }
        jdbcHelper.executeBatch(insertSQL,insertParamsList);
    }
}
