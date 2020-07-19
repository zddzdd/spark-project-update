package com.micro.bigdata.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.micro.bigdata.constant.Constants;
import com.micro.bigdata.dao.*;
import com.micro.bigdata.dao.factory.DAOFactory;
import com.micro.bigdata.domain.*;
import com.micro.bigdata.utils.*;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

public class UserVisitSessionAnalyzeSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION);
        SparkUtils.setMaster(conf);
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession sparkSession = new SparkSession(sc.sc());
        // 1.生成模拟数据
        SparkUtils.mockData(sc, sparkSession);
        // 2.创建DAO组件
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        long taskID = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION);
        Task task = taskDAO.findById(taskID);
        if (task == null) {
            System.out.println(new Date() + ": cannot find this task with id [" + task + "].");
            return;
        }
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        // 3.查询符合日期的行为数据
        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sparkSession, taskParam);
        JavaPairRDD<String, Row> sessionid2actionRDD = actionRDD.mapToPair(data -> new Tuple2<>(data.getString(2), data));
        sessionid2actionRDD = sessionid2actionRDD.persist(StorageLevel.MEMORY_ONLY());
        // 4.对session聚合
        JavaPairRDD<String, String> sessionid2AggrInfoRDD =
                aggregateBySession(sc, sparkSession, sessionid2actionRDD);
        // 按照使用者指定的筛选参数进行数据过滤
        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("", new SessionAggrStatAccumulator());
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(sessionid2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);
        filteredSessionid2AggrInfoRDD = filteredSessionid2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY());
        System.out.println(filteredSessionid2AggrInfoRDD.count());
        // 功能一：计算出各范围的session占比，并写入MySQL
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(),
                task.getTaskid());
        // 功能二：随机抽取session，并将session和session明细写入MySQL
        JavaPairRDD<String, Row> sessionid2detailRDD = getSessionid2detailRDD(
                filteredSessionid2AggrInfoRDD, sessionid2actionRDD);
        sessionid2detailRDD = sessionid2detailRDD.persist(StorageLevel.MEMORY_ONLY());

        randomExtractSession(sc, task.getTaskid(),
                filteredSessionid2AggrInfoRDD, sessionid2detailRDD);
        // 功能三：获取top10热门品类
        List<Tuple2<CategorySortKey, String>> top10CategoryList =
                getTop10Category(task.getTaskid(), sessionid2detailRDD);

        // 功能四：获取top10活跃session
        getTop10Session(sc,task.getTaskid(),top10CategoryList,sessionid2detailRDD);
        sc.close();
    }

    private static JavaPairRDD<String, String> aggregateBySession(JavaSparkContext sc, SparkSession sparkSession, JavaPairRDD<String, Row> sessionid2actionRDD) {
        // 对行为数据按session粒度进行分组
        JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = sessionid2actionRDD.groupByKey();
        // 对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
        JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(tuple_session -> {
            String sessionid = tuple_session._1;
            Iterator<Row> iterator = tuple_session._2.iterator();
            StringBuffer searchKeywordsBuffer = new StringBuffer("");
            StringBuffer clickCategoryIdsBuffer = new StringBuffer("");
            Long userid = null;
            // session的起始和结束时间
            Date startTime = null;
            Date endTime = null;
            // session访问时长
            int stepLength = 0;
            // 遍历session访问
            while (iterator.hasNext()) {
                // 提取每个访问行为的搜索词字段和点击品类字段
                Row row = iterator.next();
                if (userid == null) {
                    userid = row.getLong(1);
                }
                String searchKeyword = row.getString(5);
                Long clickCategoryId = null;
                if (row.get(6) != null) {
                    clickCategoryId = row.getLong(6);
                }
                if (StringUtils.isNotEmpty(searchKeyword)) {
                    if (!searchKeywordsBuffer.toString().contains(searchKeyword)) {
                        searchKeywordsBuffer.append(searchKeyword + ",");
                    }
                }
                if (clickCategoryId != null) {
                    if (!clickCategoryIdsBuffer.toString().contains(
                            String.valueOf(clickCategoryId))) {
                        clickCategoryIdsBuffer.append(clickCategoryId + ",");
                    }
                }
                // 计算session开始和结束时间
                Date actionTime = DateUtils.parseTime(row.getString(4));
                if (startTime == null) {
                    startTime = actionTime;
                }
                if (endTime == null) {
                    endTime = actionTime;
                }
                if (actionTime.before(startTime)) {
                    startTime = actionTime;
                }
                if (actionTime.after(endTime)) {
                    endTime = actionTime;
                }
                // 计算session访问步长
                stepLength++;
            }
            String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
            String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
            // 计算session访问时长（秒）
            long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;
            String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
                    + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                    + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
                    + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                    + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
                    + Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);
            return new Tuple2<Long, String>(userid, partAggrInfo);
        });
        // 查询用户数据，并映射成<userid,Row>的格式
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sparkSession.sql(sql).toJavaRDD();
        JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(row -> new Tuple2<>(row.getLong(0), row));
        // 将session粒度聚合数据，与用户信息进行join
        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = userid2PartAggrInfoRDD.join(userid2InfoRDD);
        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(tuple -> {
            String partAggrInfo = tuple._2._1;
            Row userInfoRow = tuple._2._2;
            String sessionid = StringUtils.getFieldFromConcatString(
                    partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
            int age = userInfoRow.getInt(3);
            String professional = userInfoRow.getString(4);
            String city = userInfoRow.getString(5);
            String sex = userInfoRow.getString(6);
            String fullAggrInfo = partAggrInfo + "|"
                    + Constants.FIELD_AGE + "=" + age + "|"
                    + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                    + Constants.FIELD_CITY + "|" + "=" + city + "|"
                    + Constants.FIELD_SEX + "|" + "=" + sex;
            return new Tuple2<>(sessionid, fullAggrInfo);
        });
        return sessionid2FullAggrInfoRDD;
    }

    private static JavaPairRDD<String, String> filterSessionAndAggrStat(
            JavaPairRDD<String, String> session2AggrInfoRDD,
            final JSONObject taskParam,
            final Accumulator<String> sessionAggrStatAccumulator) {
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds : "");
        if (_parameter.endsWith("\\|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }
        final String parameter = _parameter;
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = session2AggrInfoRDD.filter(tuple_row -> {
                    // 首先，从tuple中，获取聚合数据
                    String aggrInfo = tuple_row._2;
                    // 接着，依次按照筛选条件进行过滤
                    // 按照年龄范围进行过滤（startAge、endAge）
                    if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                        return false;
                    }
                    // 按照职业范围进行过滤（professionals）
                    // 互联网,IT,软件
                    // 互联网
                    if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL,
                            parameter, Constants.PARAM_PROFESSIONALS)) {
                        return false;
                    }

                    // 按照城市范围进行过滤（cities）
                    // 北京,上海,广州,深圳
                    // 成都
                    if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY,
                            parameter, Constants.PARAM_CITIES)) {
                        return false;
                    }

                    // 按照性别进行过滤
                    // 男/女
                    // 男，女
                    if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX,
                            parameter, Constants.PARAM_SEX)) {
                        return false;
                    }

                    // 按照搜索词进行过滤
                    // 我们的session可能搜索了 火锅,蛋糕,烧烤
                    // 我们的筛选条件可能是 火锅,串串香,iphone手机
                    // 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
                    // 任何一个搜索词相当，即通过
                    if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS,
                            parameter, Constants.PARAM_KEYWORDS)) {
                        return false;
                    }

                    // 按照点击品类id进行过滤
                    if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS,
                            parameter, Constants.PARAM_CATEGORY_IDS)) {
                        return false;
                    }

                    // 计数session
                    sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);

                    // 对session的访问时长和访问步长，进行统计，根据session对应的范围进行相应的累加计数
                    long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
                    long stepLen = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
                    calculateVisitLength(sessionAggrStatAccumulator, visitLength);
                    calculateStepLength(sessionAggrStatAccumulator, stepLen);
                    return true;
                }
        );
        return filteredSessionid2AggrInfoRDD;

    }

    private static void calculateStepLength(Accumulator<String> sessionAggrStatAccumulator, long stepLength) {
        if (stepLength >= 1 && stepLength <= 3) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
        } else if (stepLength >= 4 && stepLength <= 6) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
        } else if (stepLength >= 7 && stepLength <= 9) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
        } else if (stepLength >= 10 && stepLength <= 30) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
        } else if (stepLength > 30 && stepLength <= 60) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
        } else if (stepLength > 60) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
        }
    }

    /**
     * 计算访问时长范围
     *
     * @param sessionAggrStatAccumulator
     * @param visitLength
     */
    private static void calculateVisitLength(Accumulator<String> sessionAggrStatAccumulator, long visitLength) {
        if (visitLength >= 1 && visitLength <= 3) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
        } else if (visitLength >= 4 && visitLength <= 6) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
        } else if (visitLength >= 7 && visitLength <= 9) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
        } else if (visitLength >= 10 && visitLength <= 30) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
        } else if (visitLength > 30 && visitLength <= 60) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
        } else if (visitLength > 60 && visitLength <= 180) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
        } else if (visitLength > 180 && visitLength <= 600) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
        } else if (visitLength > 600 && visitLength <= 1800) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
        } else if (visitLength > 1800) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
        }
    }

    private static void calculateAndPersistAggrStat(String value, long taskid) {
        System.out.println("value:" + value);
        // 从Accumulator统计串中获取值
        long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT));
        long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_60));
        // 计算各个访问时长和访问步长的范围
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                (double) visit_length_1s_3s / (double) session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                (double) visit_length_4s_6s / (double) session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                (double) visit_length_7s_9s / (double) session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                (double) visit_length_10s_30s / (double) session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                (double) visit_length_30s_60s / (double) session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                (double) visit_length_1m_3m / (double) session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                (double) visit_length_3m_10m / (double) session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_10m_30m / (double) session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_30m / (double) session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                (double) step_length_1_3 / (double) session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                (double) step_length_4_6 / (double) session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                (double) step_length_7_9 / (double) session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                (double) step_length_10_30 / (double) session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                (double) step_length_30_60 / (double) session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                (double) step_length_60 / (double) session_count, 2);
        // 将统计结果封装为Domain对象
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskid);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        //调用对应的DAO插入统计结果
        ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.insert(sessionAggrStat);

    }

    /**
     * 获取通过筛选条件的session的访问明细数据RDD
     *
     * @param sessionid2aggrInfoRDD
     * @param sessionid2actionRDD
     * @return
     */
    private static JavaPairRDD<String, Row> getSessionid2detailRDD(
            JavaPairRDD<String, String> sessionid2aggrInfoRDD,
            JavaPairRDD<String, Row> sessionid2actionRDD) {
        JavaPairRDD<String, Row> sessionid2detailRDD = sessionid2aggrInfoRDD
                .join(sessionid2actionRDD)
                .mapToPair(tuple -> (new Tuple2<String, Row>(tuple._1, tuple._2._2)));
        return sessionid2detailRDD;
    }

    /**
     * 随机抽取session
     *
     * @param sessionid2AggrInfoRDD
     */
    private static void randomExtractSession(
            JavaSparkContext sc,
            final long taskid,
            JavaPairRDD<String, String> sessionid2AggrInfoRDD,
            JavaPairRDD<String, Row> sessionid2actionRDD) {
        /**
         *第一步，计算出每天每小时的session数量
         */

        JavaPairRDD<String, String> time2sessionidRDD = sessionid2AggrInfoRDD.mapToPair(tuple -> {
            String aggrInfo = tuple._2;
            String startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME);
            String dateHour = DateUtils.getDateHour(startTime);
            return new Tuple2<>(dateHour, aggrInfo);
        });
        Map<String, Long> countMap = time2sessionidRDD.countByKey();
        /**
         *第二步，使用按时间比例随机抽取算法，计算出每天每小时要抽取session的索引
         */

        // 将<yyyy-MM-dd_HH,count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式
        Map<String, Map<String, Long>> dateHourCountMap = new HashMap<>();
        for (String dateKey : countMap.keySet()) {
            Long count = countMap.get(dateKey);
            String date = dateKey.split("_")[0];
            String hour = dateKey.split("_")[1];
            Map<String, Long> hourCountMap = dateHourCountMap.get(date);
            if (hourCountMap == null) {
                hourCountMap = new HashMap<>();
                dateHourCountMap.put(date, hourCountMap);
            }
            hourCountMap.put(hour, count);
        }

        // 总共要抽取100个session，先按照天数，进行平分
        int extractNumberPerDay = 100 / dateHourCountMap.size();

        /**
         * session随机抽取功能
         */
        Map<String, Map<String, List<Integer>>> dateHourExtractMap = new HashMap<>();
        Random random = new Random();
        for (String date : dateHourCountMap.keySet()) {
            Map<String, Long> hourCountMap = dateHourCountMap.get(date);
            // 1.计算出这一天的session总数
            long sessionCount = 0L;
            for (long hourCount : hourCountMap.values()) {
                sessionCount += hourCount;
            }
            Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
            if (hourExtractMap == null) {
                hourExtractMap = new HashMap<>();
                dateHourExtractMap.put(date, hourExtractMap);
            }
            // 2.计算当前小时的session总数、占比和需要抽取的数量
            for (String hour : hourCountMap.keySet()) {
                long count = hourCountMap.get(hour);
                int hourExtractNumber = (int) (((double) count / (double) sessionCount) * extractNumberPerDay);
                if (hourExtractNumber > count) hourExtractNumber = (int) count;
                // 先获取当前小时的存放随机数的list
                List<Integer> extractIndexList = hourExtractMap.get(hour);
                if (extractIndexList == null) {
                    extractIndexList = new ArrayList<>();
                    hourExtractMap.put(hour, extractIndexList);
                }
                for (int i = 0; i < hourExtractNumber; i++) {
                    int extractIndex = random.nextInt((int) count);
                    while (extractIndexList.contains(extractIndex)) {
                        extractIndex = random.nextInt((int) count);
                    }
                    extractIndexList.add(extractIndex);
                }
            }
        }

        /**
         * 第三步：遍历每天每小时的session，然后根据随机索引进行抽取
         */
        JavaPairRDD<String, Iterable<String>> time2sessionsRDD = time2sessionidRDD.groupByKey();

        JavaPairRDD<String, String> extractSessionidsRDD = time2sessionsRDD.flatMapToPair(tuple -> {
            List<Tuple2<String, String>> extractSessionids = new ArrayList<>();
            String dateHour = tuple._1;
            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];
            Iterator<String> iterator = tuple._2.iterator();
            List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);
            int index = 0;
            ISessionRandomExtractDAO sessionRandomExtractDAO = DAOFactory.getSessionRandomExtractDAO();
            while (iterator.hasNext()) {
                String sessionAggrInfo = iterator.next();
                if (extractIndexList.contains(index)) {
                    String sessionid = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
                    String startTime = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_START_TIME);
                    String searchKeyWords = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS);
                    String click_category = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS);
                    SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                    sessionRandomExtract.setTaskid(taskid);
                    sessionRandomExtract.setSessionid(sessionid);
                    sessionRandomExtract.setClickCategoryIds(click_category);
                    sessionRandomExtract.setSearchKeywords(searchKeyWords);
                    sessionRandomExtract.setStartTime(startTime);
                    sessionRandomExtractDAO.insert(sessionRandomExtract);
                    // 将sessionid加入list
                    extractSessionids.add(new Tuple2<>(sessionid, sessionid));
                }
                index++;
            }
            return extractSessionids.iterator();
        });

        /**
         * 第四步：获取抽取出来的session的明细数据
         */
        /*JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD = extractSessionidsRDD.join(sessionid2actionRDD);
        extractSessionDetailRDD.foreachPartition(iterator -> {
            List<SessionDetail> sessionDetails = new ArrayList<SessionDetail>();
            while (iterator.hasNext()) {
                Tuple2<String, Tuple2<String, Row>> next = iterator.next();
                Row row = next._2._2;
                SessionDetail sessionDetail = new SessionDetail();
                sessionDetail.setTaskid(taskid);
                sessionDetail.setUserid(row.getLong(1));
                sessionDetail.setSessionid(row.getString(2));
                sessionDetail.setPageid(row.getLong(3));
                sessionDetail.setActionTime(row.getString(4));
                sessionDetail.setSearchKeyword(row.getString(5));
                if (row.get(6) != null) {
                    sessionDetail.setClickCategoryId(row.getLong(6));
                }
                if (row.get(7) != null)
                    sessionDetail.setClickProductId(row.getLong(7));
                sessionDetail.setOrderCategoryIds(row.getString(8));
                sessionDetail.setOrderProductIds(row.getString(9));
                sessionDetail.setPayCategoryIds(row.getString(10));
                sessionDetail.setPayProductIds(row.getString(11));
                sessionDetails.add(sessionDetail);
            }
            ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
            sessionDetailDAO.insertBatch(sessionDetails);
        });*/
    }

    public static List<Tuple2<CategorySortKey, String>> getTop10Category(
            long taskid,
            JavaPairRDD<String, Row> sessionid2detailRDD) {
        /**
         * 第一步：获取符合条件的session访问过的所有品类
         */
        JavaPairRDD<Long, Long> categoryidRDD = sessionid2detailRDD.flatMapToPair(tuple -> {
            Row row = tuple._2;
            List<Tuple2<Long, Long>> list = new ArrayList<>();
            if(row.get(6) != null){
                Long clickCategoryId = row.getLong(6);
                list.add(new Tuple2<>(clickCategoryId, clickCategoryId));
            }
            if(row.get(8) != null){
                String orderCategoryIds = row.getString(8);
                String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
                for (String orderCategoryId : orderCategoryIdsSplited) {
                    list.add(new Tuple2<>(Long.valueOf(orderCategoryId),
                            Long.valueOf(orderCategoryId)));
                }
            }
            if(row.get(10) != null){
                String payCategoryIds = row.getString(10);
                String[] payCategoryIdsSplited = payCategoryIds.split(",");
                for (String payCategoryId : payCategoryIdsSplited) {
                    list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId),
                            Long.valueOf(payCategoryId)));
                }
            }
            return list.iterator();
        });
        categoryidRDD = categoryidRDD.distinct();

        /**
         * 第二步：计算各品类的点击、下单和支付的次数
         */
        // 计算各个品类的点击次数
        JavaPairRDD<Long, Long> clickCategoryId2CountRDD =
                getClickCategoryId2CountRDD(sessionid2detailRDD);
        // 计算各个品类的下单次数
        JavaPairRDD<Long, Long> orderCategoryId2CountRDD =
                getOrderCategoryId2CountRDD(sessionid2detailRDD);
        // 计算各个品类的支付次数
        JavaPairRDD<Long, Long> payCategoryId2CountRDD =
                getPayCategoryId2CountRDD(sessionid2detailRDD);
        /**
         * 第三步：join各品类与它的点击、下单和支付的次数
         */
        JavaPairRDD<Long, String> categoryid2countRDD = joinCategoryAndData(
                categoryidRDD, clickCategoryId2CountRDD, orderCategoryId2CountRDD,
                payCategoryId2CountRDD
        );
        /**
         * 第四步：将数据映射成<CategorySortKey,info>格式的RDD，然后进行二次排序（降序）
         */
        JavaPairRDD<CategorySortKey,String> sortKey2countRDD = categoryid2countRDD.mapToPair(tuple->{
            String countInfo = tuple._2;
            long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_CLICK_COUNT));
            long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
            long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_PAY_COUNT));
            CategorySortKey sortKey = new CategorySortKey(clickCount,orderCount,payCount);
            return new Tuple2<>(sortKey,countInfo);
        });
        JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = sortKey2countRDD.sortByKey(false);

        /**
         * 第五步：用take(10)取出top10热门品类，并写入MySQL
         */
        List<Tuple2<CategorySortKey, String>> top10CategoryList =
                sortedCategoryCountRDD.take(10);
        ITop10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();
        for (Tuple2<CategorySortKey,String> tuple:top10CategoryList){
            String countInfo = tuple._2;
            long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_CATEGORY_ID));
            long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
            long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
            long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_PAY_COUNT));
            Top10Category category = new Top10Category();
            category.setTaskid(taskid);
            category.setCategoryid(categoryid);
            category.setClickCount(clickCount);
            category.setOrderCount(orderCount);
            category.setPayCount(payCount);
            top10CategoryDAO.insert(category);
        }
        return top10CategoryList;

    }

    private static JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(
            JavaPairRDD<String, Row> sessionid2detailRDD) {
        JavaPairRDD<String, Row> clickActionRDD = sessionid2detailRDD.filter(tuple -> {
            Row row = tuple._2;
            return row.get(6) != null ? true : false;
        });
        return clickActionRDD.mapToPair(tuple -> {
            long clickCategoryId = tuple._2.getLong(6);
            return new Tuple2<>(clickCategoryId, 1L);
        }).reduceByKey((Long v1, Long v2) -> v1 + v2);
    }

    private static JavaPairRDD<Long, Long> getOrderCategoryId2CountRDD(
            JavaPairRDD<String, Row> sessionid2detailRDD) {
        JavaPairRDD<String, Row> orderActionRDD = sessionid2detailRDD.filter(tuple -> {
            Row row = tuple._2;
            return row.get(8) != null ? true : false;
        });
        JavaPairRDD<Long, Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair(tuple -> {
            Row row = tuple._2;
            String orderCategoryIds = row.getString(8);
            String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
            List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
            for (String orderCategoryId : orderCategoryIdsSplited) {
                list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), 1L));
            }
            return list.iterator();
        });
        return orderCategoryIdRDD.reduceByKey((Long v1, Long v2) -> v1 + v2);
    }

    private static JavaPairRDD<Long, Long> getPayCategoryId2CountRDD(
            JavaPairRDD<String, Row> sessionid2detailRDD) {
        JavaPairRDD<String, Row> payActionRDD = sessionid2detailRDD.filter(tuple -> {
            Row row = tuple._2;
            return row.getString(10) != null ? true : false;
        });
        JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD.flatMapToPair(tuple -> {
            Row row = tuple._2;
            String payCategoryIds = row.getString(10);
            String[] payCategoryIdsSplited = payCategoryIds.split(",");
            List<Tuple2<Long, Long>> list = new ArrayList<>();
            for (String payCategoryId : payCategoryIdsSplited) {
                list.add(new Tuple2<>(Long.valueOf(payCategoryId), 1L));
            }
            return list.iterator();
        });
        return payCategoryIdRDD.reduceByKey((Long v1, Long v2) -> v1 + v2);
    }

    private static JavaPairRDD<Long, String> joinCategoryAndData(
            JavaPairRDD<Long, Long> categoryidRDD,
            JavaPairRDD<Long, Long> clickCategoryId2CountRDD,
            JavaPairRDD<Long, Long> orderCategoryId2CountRDD,
            JavaPairRDD<Long, Long> payCategoryId2CountRDD) {
        JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tmpJoinRDD =
                categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD);
        JavaPairRDD<Long, String> tmpMapRDD = tmpJoinRDD.mapToPair(tuple -> {
            Long categoryid = tuple._1;
            Optional<Long> optional = tuple._2._2;
            long clickCount = 0L;
            if (optional.isPresent()) {
                clickCount = optional.get();
            }
            String value = Constants.FIELD_CATEGORY_ID + "=" + categoryid + "|" +
                    Constants.FIELD_CLICK_COUNT + "=" + clickCount;
            return new Tuple2<>(categoryid, value);
        });
        tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryId2CountRDD).mapToPair(tuple -> {
            Long categoryid = tuple._1;
            String value = tuple._2._1;
            Optional<Long> optional = tuple._2._2;
            long orderCount = 0L;
            if (optional.isPresent()) {
                orderCount = optional.get();
            }
            value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount;
            return new Tuple2<>(categoryid, value);
        });

        tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryId2CountRDD).mapToPair(tuple -> {
            Long categoryid = tuple._1;
            String value = tuple._2._1;
            Optional<Long> optional = tuple._2._2;
            long payCount = 0L;
            if (optional.isPresent()) {
                payCount = optional.get();
            }
            value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount;
            return new Tuple2<>(categoryid, value);
        });
        return tmpMapRDD;

    }

    /**
     * 获取top10活跃session
     * @param sc
     * @param taskid
     * @param top10CategoryList
     * @param sessionid2detailRDD
     */
    private static void getTop10Session(
            JavaSparkContext sc,
            final long taskid,
            List<Tuple2<CategorySortKey,String>> top10CategoryList,
            JavaPairRDD<String,Row> sessionid2detailRDD){
        /**
         * 第一步：将top10热门品类的id，生成一份RDD
         */
        List<Tuple2<Long, Long>> top10CategoryIdList = new ArrayList<>();
        for (Tuple2<CategorySortKey,String> category:top10CategoryList){
            long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
                    category._2,"\\|",Constants.FIELD_CATEGORY_ID));
            top10CategoryIdList.add(new Tuple2<>(categoryid,categoryid));
        }
        JavaPairRDD<Long, Long> top10CategoryIdRDD = sc.parallelizePairs(top10CategoryIdList);
        /**
         *计算所有品类被各session点击的次数
         */
        JavaPairRDD<String, Iterable<Row>> sessionid2detailsRDD =
                sessionid2detailRDD.groupByKey();
        JavaPairRDD<Long, String> categoryid2sessionCountRDD = sessionid2detailsRDD.flatMapToPair(tuple->{
           String sessionid = tuple._1;
            Iterator<Row> iterator = tuple._2.iterator();
            Map<Long,Long> categoryCountMap = new HashMap<>();
            // 计算出该session，对每个品类的点击次数
            while (iterator.hasNext()){
                Row row = iterator.next();
                if(row.get(6)!= null){
                    long categoryid = row.getLong(6);
                    Long count = categoryCountMap.get(categoryid);
                    if(count == null){
                        count = 0L;
                    }
                    count++;
                    categoryCountMap.put(categoryid,count);
                }
            }
            // 返回结果，<categoryid,sessionid,count>格式
            List<Tuple2<Long,String>> list = new ArrayList<>();
            for(long categoryid:categoryCountMap.keySet()){
                Long count = categoryCountMap.get(categoryid);
                String value = sessionid+","+count;
                list.add(new Tuple2<>(categoryid,value));
            }
            return list.iterator();
        });
        /**
         * 关联top10热门品类，被各个session点击的次数
         */
        JavaPairRDD<Long, String> top10CategorySessionCountRDD = top10CategoryIdRDD.join(categoryid2sessionCountRDD)
                .mapToPair(tuple->new Tuple2<>(tuple._1,tuple._2._2));
        /**
         * 按品类分组
         */
        JavaPairRDD<Long, Iterable<String>> top10CategorySessionCountsRDD =
                top10CategorySessionCountRDD.groupByKey();
        /**
         * 遍历各品类的session次数，排序取top10
         */
        JavaPairRDD<String,String> sessionidPair = top10CategorySessionCountsRDD.flatMapToPair(tuple->{
            Long categoryid = tuple._1;
            Iterator<String> iterator = tuple._2.iterator();
            // 定义取topn的排序数组
            int n = 10;
            String[]top10Sessions = new String[n];
            while (iterator.hasNext()){
                String sessionCount = iterator.next();
                long count = Long.valueOf(sessionCount.split(",")[1]);
                // 遍历排序数组
                for (int i = 0; i < top10Sessions.length; i++) {
                    if(top10Sessions[i] == null){
                        top10Sessions[i] = sessionCount;
                        break;
                    }
                    else {
                        long _count = Long.valueOf(top10Sessions[i].split(",")[1]);
                        if(_count<count){
                            for (int j = n-1; j > i ; j--) {
                                top10Sessions[j] = top10Sessions[j-1];
                            }
                            top10Sessions[i] = sessionCount;
                            break;
                        }
                    }
                }
            }
            // 将数据写入MySQL表
            List<Tuple2<String,String>> list = new ArrayList<>();
            for(String sessionCount:top10Sessions){
                if(sessionCount != null){
                    String sessionid = sessionCount.split(",")[0];
                    long count = Long.valueOf(sessionCount.split(",")[1]);

                    // 将top10 session插入MySQL表
                    Top10Session top10Session = new Top10Session();
                    top10Session.setTaskid(taskid);
                    top10Session.setCategoryid(categoryid);
                    top10Session.setSessionid(sessionid);
                    top10Session.setClickCount(count);

                    ITop10SessionDAO top10SessionDAO = DAOFactory.getTop10SessionDAO();
                    top10SessionDAO.insert(top10Session);

                    // 放入list
                    list.add(new Tuple2<String, String>(sessionid, sessionid));
                }
            }
            return  list.iterator();
        });

        /**
         * 第四步：获取top10活跃session的明细数据，并写入MySQL
         */
        sessionidPair.join(sessionid2detailRDD).foreach(tuple->{
            Row row = tuple._2._2;
            SessionDetail sessionDetail = new SessionDetail();
            sessionDetail.setTaskid(taskid);
            sessionDetail.setUserid(row.getLong(1));
            sessionDetail.setSessionid(row.getString(2));
            sessionDetail.setPageid(row.getLong(3));
            sessionDetail.setActionTime(row.getString(4));
            sessionDetail.setSearchKeyword(row.getString(5));
            if(row.get(6) != null){
                sessionDetail.setClickCategoryId(row.getLong(6));
            }
            if(row.get(7) != null){
                sessionDetail.setClickProductId(row.getLong(7));
            }
            if(row.get(8) != null){
                sessionDetail.setOrderCategoryIds(row.getString(8));
            }
            if(row.get(10) != null){
                sessionDetail.setPayCategoryIds(row.getString(10));
            }

            sessionDetail.setOrderProductIds(row.getString(9));

            sessionDetail.setPayProductIds(row.getString(11));
            ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
            sessionDetailDAO.insert(sessionDetail);
        });


    }
}