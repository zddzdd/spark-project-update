package com.micro.bigdata.spark.page;

import com.alibaba.fastjson.JSONObject;
import com.micro.bigdata.constant.Constants;
import com.micro.bigdata.dao.IPageSplitConvertRateDAO;
import com.micro.bigdata.dao.ITaskDAO;
import com.micro.bigdata.dao.factory.DAOFactory;
import com.micro.bigdata.domain.PageSplitConvertRate;
import com.micro.bigdata.domain.Task;
import com.micro.bigdata.utils.DateUtils;
import com.micro.bigdata.utils.NumberUtils;
import com.micro.bigdata.utils.ParamUtils;
import com.micro.bigdata.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;

/**
 * 页面单跳转化率
 */
public class PageConverRate {
    public static void main(String[] args) {
        // 1、构造Spark上下文
        SparkConf conf  = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_PAGE);
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkUtils.setMaster(conf);
        SparkSession sparkSession = SparkSession.builder()
                .config("spark.sql.warehouse.dir","hdfs://master:9000/hive/warehouse")
                .enableHiveSupport()
                .getOrCreate();


        // 2、生成模拟数据
        SparkUtils.mockData(sc,sparkSession);
        // 3、查询任务，获取任务的参数
        Long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PAGE);
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(taskId);
        if(task == null){
            System.out.println(new Date()+": cannot find this task with id ["+ taskId+"].");
            return;
        }
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        // 4、查询指定日期范围内的用户访问行为数据

        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(
                sparkSession, taskParam);
        System.out.println("count:"+actionRDD.count());

        JavaPairRDD<String, Row> sessionid2actionRDD = getSessionid2actionRDD(actionRDD);
        sessionid2actionRDD = sessionid2actionRDD.cache();
        // 5、整理生成切片的数据
        // 对<sessionid,访问行为> RDD，做一次groupByKey操作
        // 因为我们要拿到每个session对应的访问行为数据，才能够去生成切片
        JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD = sessionid2actionRDD.groupByKey();
        // 6、最核心的一步，每个session的单跳页面切片的生成，以及页面流的匹配，算法
        JavaPairRDD<String, Integer> pageSplitRDD = generateAndMatchPageSplit(sc,sessionid2actionsRDD,taskParam);
        Map<String,Long>pageSplitPvMap = pageSplitRDD.countByKey();
        // 7、拿到最初的页面切片
        long startPagePv = getStartPagePv(taskParam, sessionid2actionsRDD);
        // 8、计算目标页面流的各个页面切片的转化率
        Map<String, Double> convertRateMap = computePageSplitConvertRate(
                taskParam, pageSplitPvMap, startPagePv);
        // 9、持久化页面切片转化率
        persistConvertRate(taskId, convertRateMap);
    }

    private static void persistConvertRate(Long taskId, Map<String, Double> convertRateMap) {
        StringBuffer buffer = new StringBuffer("");
        for(Map.Entry<String, Double> convertRateEntry : convertRateMap.entrySet()) {
            String pageSplit = convertRateEntry.getKey();
            double convertRate = convertRateEntry.getValue();
            buffer.append(pageSplit + "=" + convertRate + "|");
        }

        String convertRate = buffer.toString();
        convertRate = convertRate.substring(0, convertRate.length() - 1);

        PageSplitConvertRate pageSplitConvertRate = new PageSplitConvertRate();
        pageSplitConvertRate.setTaskid(taskId);
        pageSplitConvertRate.setConvertRate(convertRate);

        IPageSplitConvertRateDAO pageSplitConvertRateDAO = DAOFactory.getPageSplitConvertRateDAO();
        pageSplitConvertRateDAO.insert(pageSplitConvertRate);
    }

    private static Map<String, Double> computePageSplitConvertRate(JSONObject taskParam, Map<String, Long> pageSplitPvMap, long startPagePv) {
        Map<String, Double> convertRateMap = new HashMap<String, Double>();
        String[] targetPages = taskParam.getString("targetPageFlow").split(",");
        long lastPageSplitPv = 0L;
        for (int i = 1; i < targetPages.length; i++) {
            String targetPageSplit = targetPages[i-1]+"_"+targetPages[i];
            long targetPageSplitPv = Long.parseLong(String.valueOf(pageSplitPvMap.get(targetPageSplit)));
            double convertRate = 0.0;
            if(i==1){
                convertRate = NumberUtils.formatDouble((double) targetPageSplitPv/(double) startPagePv,2);
            }else {
                convertRate = NumberUtils.formatDouble((double)targetPageSplitPv/(double)lastPageSplitPv,2);
            }
            convertRateMap.put(targetPageSplit,convertRate);
            lastPageSplitPv = targetPageSplitPv;
        }
        return convertRateMap;
    }

    private static long getStartPagePv(JSONObject taskParam, JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD) {
        String targetPageFlow = taskParam.getString("targetPageFlow");
        final long startPageId = Long.valueOf(targetPageFlow.split(",")[0]);
        JavaRDD<Long> startPageRDD = sessionid2actionsRDD.flatMap(new FlatMapFunction<Tuple2<String, Iterable<Row>>, Long>() {
            @Override
            public Iterator<Long> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                List<Long> list = new ArrayList<Long>();
                Iterator<Row> iterator = tuple._2.iterator();
                while (iterator.hasNext()){
                    Row row = iterator.next();
                    long pageid = row.getLong(3);
                    if(pageid==startPageId){
                        list.add(pageid);
                    }
                }
                return list.iterator();
            }
        });
        return startPageRDD.count();
    }

    private static JavaPairRDD<String, Integer> generateAndMatchPageSplit(
            JavaSparkContext sc,
            JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD,
            JSONObject taskParam) {
        String targetPageFlow = taskParam.getString("targetPageFlow");
        final Broadcast<String> targetPageFlowBroadcast = sc.broadcast(targetPageFlow);
        return sessionid2actionsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                // 定义返回list
                List<Tuple2<String, Integer>> list =
                        new ArrayList<Tuple2<String, Integer>>();
                // 获取到当前session的访问行为的迭代器
                Iterator<Row> iterator = tuple._2.iterator();
                // 获取使用者指定的页面流
                String[] targetPages = targetPageFlowBroadcast.value().split(",");
                // 排序
                List<Row> rows = new ArrayList<Row>();
                while (iterator.hasNext()){
                    rows.add(iterator.next());
                }
                Collections.sort(rows, new Comparator<Row>() {
                    @Override
                    public int compare(Row o1, Row o2) {
                        String actionTime1 = o1.getString(4);
                        String actionTime2 = o2.getString(4);
                        Date date1 = DateUtils.parseTime(actionTime1);
                        Date date2 = DateUtils.parseTime(actionTime2);
                        return (int)(date1.getTime() - date2.getTime());
                    }
                });

                // 页面切片的生成，以及页面流的匹配
                Long lastPageId = null;
                for(Row row:rows){
                    long pageid = row.getLong(3);
                    if(lastPageId==null) {
                        lastPageId = pageid;
                        continue;
                    }
                    // 生成一个页面切片
                    String pageSplit = lastPageId+"_"+pageid;
                    // 对这个切片判断一下，是否在用户指定的页面流中
                    for (int i = 1; i < targetPages.length; i++) {
                        String targetPageSplit = targetPages[i-1]+"_"+targetPages[i];
                        if(pageSplit.equals(targetPageSplit)){
                            list.add(new Tuple2<>(pageSplit,1));
                            break;
                        }
                    }
                    lastPageId = pageid;
                }
                return list.iterator();
            }
        });

    }

    private static JavaPairRDD<String, Row> getSessionid2actionRDD(JavaRDD<Row> actionRDD) {
        return actionRDD.mapToPair(x->new Tuple2<>(x.getString(2),x));
    }
}
