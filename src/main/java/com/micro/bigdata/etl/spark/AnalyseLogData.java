package com.micro.bigdata.etl.spark;

import com.micro.bigdata.constant.EventLogConstants;
import com.micro.bigdata.etl.util.LoggerUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import com.micro.bigdata.constant.EventLogConstants.EventEnum;
import scala.Tuple2;


import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.zip.CRC32;



public class AnalyseLogData {
    private static final Logger logger = Logger.getLogger(AnalyseLogData.class);
    private static byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);
    private static CRC32 crc32 = new CRC32();


    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf()
                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                .setMaster("local")
                .setAppName(AnalyseLogData.class.getName());

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
            String rowkey = generateRowKey(uuid, memberId, event, serverTime);
            System.out.println(rowkey);
            Put put = new Put(Bytes.toBytes(rowkey));
            for(String key:clientInfo.keySet()){
                if(StringUtils.isNotBlank(key) && StringUtils.isNotBlank(clientInfo.get(key))){
                    put.add(family,Bytes.toBytes(key),Bytes.toBytes(clientInfo.get(key)));
                }
            }
            return put;
        }else {
            accumulator.add(1);
            return null;
        }
    }

    private static String generateRowKey(String uuid, String memberId, String eventAliasName, String serverTime) {
        StringBuilder sb = new StringBuilder();
        sb.append(serverTime).append("_");crc32.reset();
        if (StringUtils.isNotBlank(uuid)) {
            crc32.update(uuid.getBytes());
        }
        if (StringUtils.isNotBlank(memberId)) {
            crc32.update(memberId.getBytes());
        }
        crc32.update(eventAliasName.getBytes());
        sb.append(crc32.getValue() % 100000000L);
        return sb.toString();
    }
}
