package com.micro.bigdata.spark.product;

import com.alibaba.fastjson.JSONObject;
import com.micro.bigdata.conf.ConfigurationManager;
import com.micro.bigdata.constant.Constants;
import com.micro.bigdata.dao.IAreaTop3ProductDAO;
import com.micro.bigdata.dao.ITaskDAO;
import com.micro.bigdata.dao.factory.DAOFactory;
import com.micro.bigdata.dao.impl.AreaTop3ProductDAOImpl;
import com.micro.bigdata.domain.AreaTop3Product;
import com.micro.bigdata.domain.Task;
import com.micro.bigdata.utils.ParamUtils;
import com.micro.bigdata.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

/**
 * 各区域商品统计Spark作业
 */
public class AreaTop3ProductSpark {
    public static void main(String[] args) {
        // 1.创建SparkConf
        SparkConf conf = new SparkConf()
                .setAppName(AreaTop3ProductSpark.class.getName());
        SparkUtils.setMaster(conf);
        // 2.构建Spark上下文
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession sparkSession = SparkSession.builder()
                .config("spark.sql.warehouse.dir","hdfs://master:9000/hive/warehouse")
                .enableHiveSupport()
                .getOrCreate();
        // 注册自定义函数
        sparkSession.udf().register("concat_long_string",
                new ConcatLongStringUDF(), DataTypes.StringType);
        sparkSession.udf().register("group_concat_distinct",
                new GroupConcatDistinctUDAF());
        sparkSession.udf().register("get_json_object",
                new GetJsonObjectUDF(), DataTypes.StringType);
        sparkSession.udf().register("random_prefix",
                new RandomPrefixUDF(),DataTypes.StringType);
        sparkSession.udf().register("remove_random_prefix",
                new RemoveRandomPrefixUDF(),DataTypes.StringType);

        // 3.准备模拟数据
        SparkUtils.mockData(sc,sparkSession);
        // 4.获取命令行传入的taskid，查询对应的任务参数
        long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PRODUCT);
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(taskId);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        String start_date = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String end_date = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
        // 4.查询用户指定日期的点击行为数据
        JavaPairRDD<Long, Row> clickActionRDD = getClickActionRDDByDate(start_date, end_date,sparkSession);
        System.out.println("clickActionRDD："+clickActionRDD.count());
        // 5.从MySQL中查询城市信息
        JavaPairRDD<Long,Row> cityInfoRDD = getCityInfoRDD(sparkSession);
        System.out.println("cityInfoRDD："+cityInfoRDD.count());
        // 6.关联点击行为数据和城市信息数据,生成点击商品基础信息临时表
        generateTmpClickProductBassicTable(clickActionRDD,cityInfoRDD,sparkSession);
        // 7.生成各区域各商品点击次数的临时表
        generateTempAreaPrdocutClickCountTable(sparkSession);
        // 8.生成包含完整商品信息的各区域各商品点击次数的临时表
        generateTempAreaFullProductClickCountTable(sparkSession);
        // 9.使用开窗函数获取各个区域内点击次数排名前3的热门商品
        JavaRDD<Row> areaTop3ProductRDD = getAreaTop3ProductRDD(sparkSession);
        System.out.println("areaTop3ProductRDD:"+areaTop3ProductRDD.count());

        // 10.批量插入mysql
        List<Row> rows = areaTop3ProductRDD.collect();
        System.out.println("rows:"+rows.size());
        persistAreaTop3Product(taskId,rows);
        sc.close();
    }

    private static void persistAreaTop3Product(long taskId, List<Row> rows) {
        List<AreaTop3Product> areaTop3Products = new ArrayList<>();
        for (Row row:rows){
            AreaTop3Product areaTop3Product = new AreaTop3Product();
            areaTop3Product.setTaskid(taskId);
            areaTop3Product.setArea(row.getString(0));
            areaTop3Product.setAreaLevel(row.getString(1));
            areaTop3Product.setProductid(row.getLong(2));
            areaTop3Product.setClickCount(Long.valueOf(String.valueOf(row.get(3))));
            areaTop3Product.setCityInfos(row.getString(4));
            areaTop3Product.setProductName(row.getString(5));
            areaTop3Product.setProductStatus(row.getString(6));
            areaTop3Products.add(areaTop3Product);
        }
        IAreaTop3ProductDAO areaTop3ProductDAO = new AreaTop3ProductDAOImpl();
        areaTop3ProductDAO.insertBatch(areaTop3Products);
    }

    /**
     * 查询指定日期范围内的点击行为数据
     * @param startDate
     * @param endDate
     * @return
     */
    private static JavaPairRDD<Long,Row> getClickActionRDDByDate(String startDate,String endDate,SparkSession sparkSession){
        String sql = "select city_id,click_product_id product_id " +
                "from user_visit_action " +
                "where click_product_id is not null " +
                "and date>='"+startDate+"' "+
                "and date<='"+endDate+"' ";
        Dataset<Row> clickActionDF = null;
        try {
            clickActionDF = sparkSession.sql(sql);
        }catch (Exception e){
            e.printStackTrace();
        }
        JavaRDD<Row> clickActionRDD = clickActionDF.toJavaRDD();
        JavaPairRDD<Long, Row> cityid2clickActionRDD=clickActionRDD.mapToPair(row -> {
            Long cityid = Long.valueOf(String.valueOf(row.get(0)));
            return new Tuple2<Long,Row>(cityid,row);
        });
        return cityid2clickActionRDD;
    }

    private static JavaPairRDD<Long,Row> getCityInfoRDD(SparkSession sparkSession){
        // 构建MySQL连接配置信息
        String url = null;
        String user = null;
        String password = null;
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if(local){
            url = ConfigurationManager.getProperty(Constants.JDBC_URL);
            user = ConfigurationManager.getProperty(Constants.JDBC_USER);
            password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
        }else {
            url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
            user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
            password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
        }
        Map<String,String> options = new HashMap<>();
        options.put("url",url);
        options.put("dbtable","city_info");
        options.put("user",user);
        options.put("password",password);
        // 通过sparksession从MySQL中查询数据
        Dataset<Row> cityInfo = sparkSession.read().format("jdbc").options(options).load();
        JavaRDD<Row> cityInfoRDD = cityInfo.toJavaRDD();
        JavaPairRDD<Long,Row> cityid2cityInfoRDD = cityInfoRDD.mapToPair(row->{
            long cityid = Long.valueOf(String.valueOf(row.get(0)));
            return new Tuple2<>(cityid,row);
        });
        return cityid2cityInfoRDD;
    }

    /**
     * 关联点击行为数据和城市信息数据
     * @param clickActionRDD
     * @param cityInfoRDD
     */
    private static void generateTmpClickProductBassicTable(JavaPairRDD<Long, Row> clickActionRDD,
                                                   JavaPairRDD<Long,Row> cityInfoRDD,SparkSession sparkSession){
        JavaPairRDD<Long,Tuple2<Row,Row>> joinedRDD = clickActionRDD.join(cityInfoRDD);
        System.out.println("joinedRDD:"+joinedRDD.count());
        JavaRDD<Row> mappedRDD = joinedRDD.map(data->{
            long cityid = data._1;
            Row clickAction = data._2._1;
            Row cityInfo = data._2._2;
            long productid = clickAction.getLong(1);
            String cityName = cityInfo.getString(1);
            String area = cityInfo.getString(2);
            return RowFactory.create(cityid,cityName,area,productid);
        });
        System.out.println("mappedRDD:"+mappedRDD.count());
        mappedRDD.foreach(x-> System.out.println(x));

        // 基于JavaRDD<Row>的格式，就可以将其转换为DataFrame
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("city_id",DataTypes.LongType,true));
        structFields.add(DataTypes.createStructField("city_name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("area",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("product_id",DataTypes.LongType,true));

        StructType schema = DataTypes.createStructType(structFields);
        Dataset<Row> df = sparkSession.createDataFrame(mappedRDD, schema);
        System.out.println("tmp_click_product_basic:"+df.count());

        // 将DataFrame中的数据，注册成临时表（tmp_clk_prod_basic）
        df.registerTempTable("tmp_click_product_basic");

    }

    private static void generateTempAreaPrdocutClickCountTable(SparkSession sparkSession){
        // 按照area和product_id两个字段进行分组
        // 计算出各区域各商品的点击次数
        // 可以获取到每个area下的每个product_id的城市信息拼接起来的串
        String sql =
                "SELECT "
                        + "area,"
                        + "product_id,"
                        + "count(*) click_count, "
                        + "group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos "
                        + "FROM tmp_click_product_basic "
                        + "GROUP BY area,product_id ";

        Dataset<Row> area_df = sparkSession.sql(sql);
        System.out.println("area_df:"+area_df.count());
        // 再次将查询出来的数据注册为一个临时表
        // 各区域各商品的点击次数（以及额外的城市列表）
        area_df.registerTempTable("tmp_area_product_click_count");
    }

    /**
     * 生成区域商品点击次数临时表（包含了商品的完整信息）
     * @param sparkSession
     */
    private static void generateTempAreaFullProductClickCountTable(SparkSession sparkSession){
        // 将之前得到的各区域各商品点击次数表，product_id,
        // 去关联商品信息表，product_id，product_name和product_status
        String sql =
                "SELECT "
                        + "tapcc.area,"
                        + "tapcc.product_id,"
                        + "tapcc.click_count,"
                        + "tapcc.city_infos,"
                        + "pi.product_name,"
                        + "if(get_json_object(pi.extend_info,'product_status')='0','Self','Third Party') product_status "
                        + "FROM tmp_area_product_click_count tapcc "
                        + "JOIN product_info pi ON tapcc.product_id=pi.product_id ";
        Dataset<Row> df = null;
        try {
            df = sparkSession.sql(sql);
        }catch (Exception e){
            e.printStackTrace();
        }

        df.registerTempTable("tmp_area_fullprod_click_count");
    }

    /**
     * 获取各区域热门商品
     * @param sparkSession
     * @return
     */
    private static JavaRDD<Row> getAreaTop3ProductRDD(SparkSession sparkSession){
        String sql =
                "SELECT "
                        + "area,"
                        + "CASE "
                        + "WHEN area='China North' OR area='China East' THEN 'A Level' "
                        + "WHEN area='China South' OR area='China Middle' THEN 'B Level' "
                        + "WHEN area='West North' OR area='West South' THEN 'C Level' "
                        + "ELSE 'D Level' "
                        + "END area_level,"
                        + "product_id,"
                        + "click_count,"
                        + "city_infos,"
                        + "product_name,"
                        + "product_status "
                        + "FROM ("
                        + "SELECT "
                        + "area,"
                        + "product_id,"
                        + "click_count,"
                        + "city_infos,"
                        + "product_name,"
                        + "product_status,"
                        + "row_number() OVER (PARTITION BY area ORDER BY click_count DESC) rank "
                        + "FROM tmp_area_fullprod_click_count "
                        + ") t "
                        + "WHERE rank<=3";
        Dataset<Row> df = null;
        try {
           df = sparkSession.sql(sql);
        }catch (Exception e){
            e.printStackTrace();
        }

        return df.javaRDD();
    }

}
