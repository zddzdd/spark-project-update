package com.micro.bigdata.spark.product;

import org.apache.spark.sql.api.java.UDF1;

public class RemoveRandomPrefixUDF implements UDF1<String,String> {
    @Override
    public String call(String s) throws Exception {
        String []valSplited = s.split("_");
        return valSplited[1];
    }
}
