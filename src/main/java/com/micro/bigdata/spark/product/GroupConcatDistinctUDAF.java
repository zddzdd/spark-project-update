package com.micro.bigdata.spark.product;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

public class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction {
    // 指定输入数据的字段与类型
    private StructType inputSchema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("cityInfo",DataTypes.StringType,true)
    ));
    // 指定缓冲数据的字段与类型
    private StructType bufferSchema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("bufferCityInfo",DataTypes.StringType,true)
    ));
    // 指定返回类型
    private DataType dataType = DataTypes.StringType;
    // 确定是否是确定性的
    private boolean deterministic = true;

    @Override
    public StructType inputSchema() {
        return inputSchema;
    }

    @Override
    public StructType bufferSchema() {
        return bufferSchema;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public boolean deterministic() {
        return deterministic;
    }

    /**
     * 初始化，在内部指定一个初始值
     * @param buffer
     */
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0,"");
    }

    /**
     * 更新，实现拼接逻辑
     * @param buffer
     * @param input
     */
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        // 缓冲过的已经拼接过的城市信息串
        String bufferCityInfo = buffer.getString(0);
        String cityInfo = input.getString(0);
        //去重
        // 判断：之前没有拼接过某个城市信息，那么这里才可以接下来拼接新的城市信息
        if(!bufferCityInfo.contains(cityInfo)){
            if("".equals(bufferCityInfo)){
                bufferCityInfo += cityInfo;
            } else {
                bufferCityInfo += "," + cityInfo;
            }
            buffer.update(0,bufferCityInfo);
        }
    }

    /**
     * 合并
     * @param buffer1
     * @param buffer2
     */
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        String bufferCityInfo1 = buffer1.getString(0);
        String bufferCityInfo2 = buffer2.getString(0);
        for (String cityInfo:bufferCityInfo2.split(",")){
            if(!bufferCityInfo1.contains(cityInfo)){
                if ("".equals(bufferCityInfo1)) {
                    bufferCityInfo1+=cityInfo;
                }else {
                    bufferCityInfo1 +=","+ cityInfo;
                }

            }
        }
        buffer1.update(0,bufferCityInfo1);
    }

    @Override
    public Object evaluate(Row buffer) {
        return buffer.getString(0);
    }
}
