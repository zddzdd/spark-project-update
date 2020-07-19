package com.micro.bigdata.spark.ad;

import com.micro.bigdata.conf.ConfigurationManager;
import com.micro.bigdata.constant.Constants;
import com.micro.bigdata.dao.IAdBlacklistDAO;
import com.micro.bigdata.dao.IAdProvinceTop3DAO;
import com.micro.bigdata.dao.IAdStatDAO;
import com.micro.bigdata.dao.IAdUserClickCountDAO;
import com.micro.bigdata.dao.factory.DAOFactory;
import com.micro.bigdata.dao.IAdClickTrendDAO;
import com.micro.bigdata.domain.*;
import com.micro.bigdata.utils.DateUtils;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

public class AdClickRealTimeStatSpark {
    public static void main(String[] args) throws InterruptedException {
        // 构建Spark Streaming上下文
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("AdClickRealTimeStatSpark");
        JavaStreamingContext jssc = new JavaStreamingContext(
                conf, Durations.seconds(5));
        jssc.checkpoint("file:///D:/develop/");

        Map<String,String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));
        // 构建topic set
        // 构建topic set
        String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
        String[] kafkaTopicsSplited = kafkaTopics.split(",");
        Set<String> topics = new HashSet<>();
        for(String kafkaTopic : kafkaTopicsSplited) {
            topics.add(kafkaTopic);
        }
        // 基于kafka direct api模式，构建出了针对kafka集群中指定topic的输入DStream
        // 两个值，val1，val2；val1没有什么特殊的意义；val2中包含了kafka topic中的一条一条的实时日志数据
        JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics);
        adRealTimeLogDStream.print();
        // 根据动态黑名单进行数据过滤
        JavaPairDStream<String, String> filteredAdRealTimeLogDStream =
                filterByBlacklist(adRealTimeLogDStream);
        // 生成动态黑名单
        generateDynamicBlackList(filteredAdRealTimeLogDStream);

        // 业务功能一：计算广告点击流量实时统计结果（yyyyMMdd_province_city_adid,clickCount）
        JavaPairDStream<String, Long> adRealTimeStatDStream = calculateRealTimeStat(
                filteredAdRealTimeLogDStream);
        // 业务功能二：实时统计每天每个省份top3热门广告
        calculateProvinceTop3Ad(adRealTimeStatDStream);
        // 业务三：实时统计每天每个广告在最近1小时的滑动窗口内的点击趋势（每分钟的点击量）
        calculateAdClickCountByWindow(adRealTimeLogDStream);
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }

    private static JavaPairDStream<String, String> filterByBlacklist(JavaPairInputDStream<String, String> adRealTimeLogDStream) {
        // 刚刚接受到原始的用户点击行为日志之后
        // 根据mysql中的动态黑名单，进行实时的黑名单过滤（黑名单用户的点击行为，直接过滤掉，不要了）
        // 使用transform算子（将dstream中的每个batch RDD进行处理，转换为任意的其他RDD，功能很强大）
        JavaPairDStream<String, String> filteredAdRealTimeLogDStream = adRealTimeLogDStream.transformToPair(rdd->{
            IAdBlacklistDAO adBlacklistDAO = DAOFactory.getAdBlacklistDAO();
            List<AdBlacklist> adBlacklists = adBlacklistDAO.findAll();
            List<Tuple2<Long, Boolean>> tuples = new ArrayList<Tuple2<Long, Boolean>>();
            for(AdBlacklist adBlacklist : adBlacklists) {
                tuples.add(new Tuple2<>(adBlacklist.getUserid(), true));
            }

            JavaSparkContext sc = new JavaSparkContext(rdd.context());
            JavaPairRDD<Long, Boolean> blacklistRDD = sc.parallelizePairs(tuples);
            // 将原始数据rdd映射成<userid, tuple2<string, string>>
            JavaPairRDD<Long, Tuple2<String, String>> mappedRDD = rdd.mapToPair(tuple->{
                    String log = tuple._2;
                    String[] logSplited = log.split(" ");
                    long userid = Long.valueOf(logSplited[3]);
                    return new Tuple2<Long, Tuple2<String, String>>(userid, tuple);
            });
            // 将原始日志数据rdd，与黑名单rdd，进行左外连接
            // 如果说原始日志的userid，没有在对应的黑名单中，join不到，左外连接
            // 用inner join，内连接，会导致数据丢失
            JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> joinedRDD = mappedRDD.leftOuterJoin(blacklistRDD);
            JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> filteredRDD = joinedRDD.filter(tuple->{
                Optional<Boolean> op = tuple._2._2;
                if(op.isPresent() && op.get()){
                    return false;
                }
                return true;
            });
            JavaPairRDD<String, String> resultRDD = filteredRDD.mapToPair(tuple->tuple._2._1);
            return  resultRDD;
        });
        return filteredAdRealTimeLogDStream;
    }

    private static void generateDynamicBlackList(
            JavaPairDStream<String,String> filteredAdRealTimeLogDStream){
        // 一条一条的实时日志
        // 某个时间点 某个省份 某个城市 某个用户 某个广告(timestamp province city userid adid)
        // 计算出每5个秒内的数据中，每天每个用户每个广告的点击量
        // 通过对原始实时日志的处理
        // 将日志的格式处理成<yyyyMMdd_userid_adid, 1L>格式
        JavaPairDStream<String, Long> dailyUserAdClickDStream = filteredAdRealTimeLogDStream.mapToPair(tuple->{
            // 从tuple中获取到每一条原始的实时日志
            String log = tuple._2;
            String[] logSplited = log.split(" ");

            // 提取出日期（yyyyMMdd）、userid、adid
            String timestamp = logSplited[0];
            Date date = new Date(Long.valueOf(timestamp));
            String datekey = DateUtils.formatDateKey(date);

            long userid = Long.valueOf(logSplited[3]);
            long adid = Long.valueOf(logSplited[4]);

            // 拼接key
            String key = datekey + "_" + userid + "_" + adid;
            return new Tuple2<>(key,1L);
        });
        // 针对处理后的日志格式，执行reduceByKey算子即可
        // （每个batch中）每天每个用户对每个广告的点击量
        JavaPairDStream<String, Long> dailyUserAdClickCountDStream =
                dailyUserAdClickDStream.reduceByKey((v1,v2)->(v1+v2));
        dailyUserAdClickCountDStream.foreachRDD(rdd->{
            rdd.foreachPartition(iterator->{
                List<AdUserClickCount> adUserClickCounts = new ArrayList<AdUserClickCount>();
                while (iterator.hasNext()){
                    Tuple2<String, Long> tuple = iterator.next();
                    String[] keySplited = tuple._1.split("_");
                    String date = null;
                    long userid = 0L;
                    long adid = 0L;
                    long clickCount = 0L;
                    String s = null;
                    Date dateKey = null;
                    try {
                        s = keySplited[0];
                        dateKey = DateUtils.parseDateKey(s);
                        date = DateUtils.formatDate(dateKey);
                    }catch (Exception e){
                        System.out.println(Arrays.toString(keySplited));
                        e.printStackTrace();
                    }

                    // yyyy-MM-dd
                    userid = Long.valueOf(keySplited[1]);
                    adid = Long.valueOf(keySplited[2]);
                    clickCount = tuple._2;
                    AdUserClickCount adUserClickCount = new AdUserClickCount();
                    adUserClickCount.setDate(date);
                    adUserClickCount.setUserid(userid);
                    adUserClickCount.setAdid(adid);
                    adUserClickCount.setClickCount(clickCount);
                    adUserClickCounts.add(adUserClickCount);
                }
                IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
                adUserClickCountDAO.updateBatch(adUserClickCounts);
            });
        });
        JavaPairDStream<String, Long> blacklistDStream = dailyUserAdClickCountDStream.filter(tuple->{
            String key = tuple._1;
            String[]keySplited = key.split("_");
            String date = null;
            long userid = 0L;
            long adid = 0L;
            String s = null;
            Date dateKey = null;
            try {
                s = keySplited[0];
                dateKey = DateUtils.parseDateKey(s);
                date = DateUtils.formatDate(dateKey);
            }catch (Exception e){
                System.out.println(Arrays.toString(keySplited));
                e.printStackTrace();
            }
            userid = Long.valueOf(keySplited[1]);
            adid  = Long.valueOf(keySplited[2]);

            // 从mysql中查询指定日期指定用户对指定广告的点击量
            IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
            int clickCount = adUserClickCountDAO.findClickCountByMultiKey(date,userid,adid);

            if(clickCount >= 100){
                return true;
            }
            return false;
        });

        JavaDStream<Long> blacklistUseridDStream = blacklistDStream.map(tuple->{
            String key = tuple._1;
            String[]keySplited = key.split("_");
            Long userid = Long.valueOf(keySplited[1]);
            return userid;
        });

        JavaDStream<Long> distinctBlacklistUseridDStream =
                blacklistUseridDStream.transform(rdd->rdd.distinct());
        // 将黑名单写入MySQL
        distinctBlacklistUseridDStream.foreachRDD(rdd->{
            rdd.foreachPartition(iterator->{
                List<AdBlacklist> adBlacklists = new ArrayList<>();
                while (iterator.hasNext()){
                    long userid = iterator.next();
                    AdBlacklist adBlacklist = new AdBlacklist();
                    adBlacklist.setUserid(userid);
                }
                IAdBlacklistDAO adBlacklistDAO = DAOFactory.getAdBlacklistDAO();
                adBlacklistDAO.insertBatch(adBlacklists);
            });
        });
    }

    /**
     * 计算广告点击流量实时统计
     * @param filteredAdRealTimeLogDStream
     * @return
     */
    private static JavaPairDStream<String,Long> calculateRealTimeStat(
            JavaPairDStream<String,String> filteredAdRealTimeLogDStream){
        JavaPairDStream<String, Long> mappedDStream =
                filteredAdRealTimeLogDStream.mapToPair(tuple->{
            String log = tuple._2;
            String[] logSplited = log.split(" ");
            String timestamp = logSplited[0];
            Date date = new Date(Long.valueOf(timestamp));
            String datekey = DateUtils.formatDateKey(date);
            String province = logSplited[1];
            String city = logSplited[2];
            long adid = Long.valueOf(logSplited[4]);
            String key = datekey + "_" + province + "_" + city + "_" + adid;
            return new Tuple2<String, Long>(key, 1L);
        });
        JavaPairDStream<String, Long> aggregatedDStream =
                mappedDStream.updateStateByKey((List<Long>values,Optional<Long>optional)->{
            long clickCount = 0L;
            if(optional.isPresent()){
                clickCount = optional.get();
            }
            for(Long value:values){
                clickCount += value;
            }
            return Optional.of(clickCount);

        });
        // 将计算出来的最新结果，同步一份到mysql中，以便于j2ee系统使用
        aggregatedDStream.foreachRDD(rdd->{
            rdd.foreachPartition(iterator->{
                List<AdStat> adStats = new ArrayList<>();
                while (iterator.hasNext()){
                    Tuple2<String, Long> tuple = iterator.next();
                    String[] keySplited = tuple._1.split("_");
                    String date = keySplited[0];
                    String province = keySplited[1];
                    String city = keySplited[2];
                    long adid = Long.valueOf(keySplited[3]);

                    long clickCount = tuple._2;

                    AdStat adStat = new AdStat();
                    adStat.setDate(date);
                    adStat.setProvince(province);
                    adStat.setCity(city);
                    adStat.setAdid(adid);
                    adStat.setClickCount(clickCount);

                    adStats.add(adStat);
                }
                IAdStatDAO adStatDAO = DAOFactory.getAdStatDAO();
                adStatDAO.updateBatch(adStats);
            });
        });
        return aggregatedDStream;
    }

    /**
     * 计算每天各省份的top3热门广告
     * @param adRealTimeStatDStream
     */
    private static void calculateProvinceTop3Ad(
            JavaPairDStream<String,Long> adRealTimeStatDStream){
        JavaDStream<Row> rowsDStream = adRealTimeStatDStream.transform(rdd->{
            //计算出每天各省份各广告的点击量
            JavaPairRDD<String, Long> mappedRDD = rdd.mapToPair(tuple->{
                String[] keySplited = tuple._1.split("_");
                String date = keySplited[0];
                String province = keySplited[1];
                long adid = Long.valueOf(keySplited[3]);
                long clickCount = tuple._2;

                String key = date + "_" + province + "_" + adid;
                return new Tuple2<>(key,clickCount);
            });
            JavaPairRDD<String, Long> dailyAdClickCountByProvinceRDD =
                    mappedRDD.reduceByKey((v1, v2) -> (v1 + v2));

            JavaRDD<Row> rowsRDD = dailyAdClickCountByProvinceRDD.map(tuple->{
                String[] keySplited = tuple._1.split("_");
                String dateKey = keySplited[0];
                String province = keySplited[1];
                long adid = Long.valueOf(keySplited[2]);
                Long clickCount = tuple._2;
                String date = DateUtils.formatDate(DateUtils.parseDateKey(dateKey));
                return RowFactory.create(date,province,adid,clickCount);
            });
            StructType schma = DataTypes.createStructType(Arrays.asList(
                    DataTypes.createStructField("date",DataTypes.StringType,true),
                    DataTypes.createStructField("province", DataTypes.StringType, true),
                    DataTypes.createStructField("ad_id", DataTypes.LongType, true),
                    DataTypes.createStructField("click_count", DataTypes.LongType, true)));
            SparkSession sparkSession = new SparkSession(rdd.context());
            Dataset<Row> dailyAdClickCountByProvinceDF = sparkSession.createDataFrame(rowsRDD, schma);
            dailyAdClickCountByProvinceDF.registerTempTable("tmp_daily_ad_click_count_by_prov");
            // 使用Spark SQL执行SQL语句，配合开窗函数，统计出各身份top3热门的广告
            Dataset<Row> provinceTop3AdDF = sparkSession.sql(
                    "select "
                    +"date,"
                    +"province,"
                    +"ad_id,"
                    +"click_count "
                    +"from ( "
                        +"select "
                            +"date,"
                            +"province,"
                            +"ad_id,"
                            +"click_count,"
                            +"row_number() over(partition by province order by click_count desc) rank "
                        +"from tmp_daily_ad_click_count_by_prov "
                    +") t "
                    +"where rank>=3"
            );
            return provinceTop3AdDF.toJavaRDD();
        });
        // 将其中的数据批量更新到MySQL中
        rowsDStream.foreachRDD(rdd->{
            rdd.foreachPartition(iterator->{
                List<AdProvinceTop3> adProvinceTop3s = new ArrayList<AdProvinceTop3>();
                while (iterator.hasNext()){
                    Row row = iterator.next();
                    String date = row.getString(0);
                    String province = row.getString(1);
                    long adid = row.getLong(2);
                    long clickCount = row.getLong(3);

                    AdProvinceTop3 adProvinceTop3 = new AdProvinceTop3();
                    adProvinceTop3.setDate(date);
                    adProvinceTop3.setProvince(province);
                    adProvinceTop3.setAdid(adid);
                    adProvinceTop3.setClickCount(clickCount);

                    adProvinceTop3s.add(adProvinceTop3);
                }
                IAdProvinceTop3DAO adProvinceTop3DAO = DAOFactory.getAdProvinceTop3DAO();
                if(adProvinceTop3s.size()>0){
                    try {
                        adProvinceTop3DAO.updateBatch(adProvinceTop3s);
                    }catch (Exception e){
                        System.out.println("adProvinceTop3s:"+adProvinceTop3s.toString());
                    }
                }

            });
        });
    }

    /**
     * 计算最近1小时滑动窗口内的广告点击趋势
     * @param adRealTimeLogDStream
     */
    private static void calculateAdClickCountByWindow(
            JavaPairDStream<String,String> adRealTimeLogDStream){
        // 映射成<yyyyMMddHHMM_adid,1L>格式
        JavaPairDStream<String, Long> pairDStream = adRealTimeLogDStream.mapToPair(tuple->{
            String[] logSplited = tuple._2.split(" ");
            String timeMi = DateUtils.formatTimeMinute(new Date(Long.valueOf(logSplited[0])));
            long adid = Long.valueOf(logSplited[4]);
            return new Tuple2<>(timeMi+"_"+adid,1L);
        });
        JavaPairDStream<String,Long> aggrRDD =
                pairDStream.reduceByKeyAndWindow((v1,v2)->
                        (v1+v2),Durations.minutes(60),Durations.seconds(10));

        aggrRDD.foreachRDD(rdd->{
            rdd.foreachPartition(iterator->{
                List<AdClickTrend> adClickTrends = new ArrayList<>();
                IAdClickTrendDAO adClickTrendDAO = DAOFactory.getAdClickTrendDAO();
                while (iterator.hasNext()){
                    Tuple2<String, Long> tuple = iterator.next();
                    String[] keySplited = tuple._1.split("_");
                    // yyyyMMddHHmm
                    String dateMinute = keySplited[0];
                    long adid = Long.valueOf(keySplited[1]);
                    long clickCount = tuple._2;
                    String date = DateUtils.formatDate(DateUtils.parseDateKey(
                            dateMinute.substring(0,8)
                    ));
                    String hour = dateMinute.substring(8,10);
                    String minute = dateMinute.substring(10);
                    AdClickTrend adClickTrend = new AdClickTrend();
                    adClickTrend.setDate(date);
                    adClickTrend.setHour(hour);
                    adClickTrend.setMinute(minute);
                    adClickTrend.setAdid(adid);
                    adClickTrend.setClickCount(clickCount);
                    adClickTrends.add(adClickTrend);
                }
                adClickTrendDAO.updateBatch(adClickTrends);
            });
        });

    }
}
