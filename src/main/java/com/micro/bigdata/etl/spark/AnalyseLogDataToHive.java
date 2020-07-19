package com.micro.bigdata.etl.spark;

import com.micro.bigdata.constant.EventLogConstants;
import com.micro.bigdata.etl.util.LoggerUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Map;
import java.util.zip.CRC32;


public class AnalyseLogDataToHive {
    private static final Logger logger = Logger.getLogger(AnalyseLogDataToHive.class);
    private static byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);


    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf()
                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                .setMaster("local")
                .setAppName(AnalyseLogDataToHive.class.getName());

        JavaSparkContext sc = new JavaSparkContext(conf);


        final Accumulator accumulator = sc.accumulator(0);

//        JavaRDD<Map<String,String>> lines = sc.textFile("/log/20200610/FlumeData.*")
        JavaRDD<Map<String,String>> lines = sc.textFile("data/access.log")
                .map(logText->{
            // 解析日志
            return LoggerUtil.handleLog(logText);
        });
        JavaRDD<Map<String,String>> filter_lines = lines.filter(value->{
            if(value.isEmpty()){
                accumulator.add(1);
                return false;
            }
            return true;
        });
        filter_lines.foreach(clientInfo->{
            Put put = handleData(clientInfo, "e_e", accumulator);
            Configuration hbaseConf = HBaseConfiguration.create();
            //设置zooKeeper集群地址
            hbaseConf.set("hbase.zookeeper.quorum","master,slave1,slave2");
            //设置zookeeper连接端口，默认2181
            hbaseConf.set("hbase.zookeeper.property.clientPort","2181");
            String table = "eventlog";
            HTable hTable = new HTable(hbaseConf, table);
            hTable.setAutoFlush(false,false);
            hTable.setWriteBufferSize(3*1024*1024);
            hTable.put(put);
            hTable.flushCommits();
        });
    }

    private static Put handleData(Map<String, String> clientInfo, String event,Accumulator accumulator) {
        String uuid = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_UUID);
        String memberId = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID);
        String serverTime = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME);
        if (StringUtils.isNotBlank(serverTime)) {
            clientInfo.remove(EventLogConstants.LOG_COLUMN_NAME_USER_AGENT); // 浏览器信息去掉
            for(String key:clientInfo.keySet()){
                if(StringUtils.isNotBlank(key) && StringUtils.isNotBlank(clientInfo.get(key))){
                    String value = clientInfo.get(key);

                }
            }
            return null;
//            return put;
        }else {
            accumulator.add(1);
            return null;
        }
    }

}
