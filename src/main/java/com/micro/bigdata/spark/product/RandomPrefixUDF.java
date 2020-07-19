package com.micro.bigdata.spark.product;

import org.apache.spark.sql.api.java.UDF2;

import java.util.Random;

/**
 * random_prefix()
 */
public class RandomPrefixUDF implements UDF2<String,Integer,String> {
    @Override
    public String call(String s, Integer integer) throws Exception {
        Random random = new Random();
        int randNum = random.nextInt(10);
        return randNum + "_" +s;
    }
}
