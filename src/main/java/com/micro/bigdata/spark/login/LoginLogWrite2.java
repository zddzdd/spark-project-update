package com.micro.bigdata.spark.login;

import com.micro.bigdata.dao.IOrgSwitchDAO;
import com.micro.bigdata.dao.IUserLoginLogDao;
import com.micro.bigdata.dao.factory.DAOFactory;
import com.micro.bigdata.domain.OrgSwitch;
import com.micro.bigdata.domain.UserLoginLog;
import com.micro.bigdata.utils.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple12;

import java.sql.Timestamp;
import java.util.Date;

public class LoginLogWrite2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName(LoginLogWrite2.class.getName());
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("data/access2.log");
        JavaRDD<Tuple12<String,String,String,String,String,String,String,String,String,String,String,String>> ori_rdd = lines.map(line->{
            String org_id = "";
            String event_name = "";
            String uid = "";
            String platform_code = "";
            String client_type = "";
            String login_type = "";
            String app_version = "";
            String phone = "";
            String hw = "";
            String nw = "";
            String login_host = "";
            String login_time = "";
            try {
                String params = line.split("\\?")[1];
                String request_info = line.split("\\?")[0];
                uid = StringUtils.getFieldFromConcatString(params, "&", "uid");
                org_id = StringUtils.getFieldFromConcatString(params,"&","org_id");
                platform_code = StringUtils.getFieldFromConcatString(params, "&", "p_c");
                client_type = StringUtils.getFieldFromConcatString(params,"&","c_t");
                login_type = StringUtils.getFieldFromConcatString(params,"&","login_type");
                app_version = StringUtils.getFieldFromConcatString(params,"&","vc");
                phone = StringUtils.getFieldFromConcatString(params,"&","phone");
                hw = StringUtils.getFieldFromConcatString(params,"&","hw");
                nw = StringUtils.getFieldFromConcatString(params,"&","nw");
                login_host = request_info.split("^A")[0];
                login_time = StringUtils.getFieldFromConcatString(params, "&", "t");
                event_name = StringUtils.getFieldFromConcatString(params, "&", "event_name");
            }catch (Exception e){
                System.out.println(line);
                e.printStackTrace();
            }
            return new Tuple12<>(uid,org_id,platform_code,client_type,login_type,app_version,phone,hw,nw,login_host,login_time,event_name);
        });
        ori_rdd = ori_rdd.persist(StorageLevel.MEMORY_ONLY());
        System.out.println(ori_rdd.count());

        JavaRDD<Tuple12<String, String, String, String, String, String, String, String, String, String, String, String>> login_rdd =
                ori_rdd.filter(x -> x._12().equals("login_org"));
        System.out.println(login_rdd.count());
        login_rdd.foreach(x->{
            long time = Long.valueOf(x._11());
            Date date = new Date(time);
            Timestamp timestamp = new Timestamp(date.getTime());
            UserLoginLog userLoginLog = new UserLoginLog(x._1(),x._2(),x._3(),x._4(),x._5(),x._6(),x._7(),x._8(),x._9(),x._10(),timestamp);
            IUserLoginLogDao userLoginLogDao = DAOFactory.getUserLoginLogDao();
            userLoginLogDao.insert(userLoginLog);
        });

        JavaRDD<Tuple12<String, String, String, String, String, String, String, String, String, String, String, String>> org_rdd =
                ori_rdd.filter(x -> x._12().equals("exchange_org"));
        System.out.println(org_rdd.count());
        org_rdd.foreach(x->{
            String user_id = x._1();
            String org_id = x._2();
            long time = Long.valueOf(x._11());
            Date date = new Date(time);
            Timestamp timestamp = new Timestamp(date.getTime());
            OrgSwitch orgSwitch = new OrgSwitch(user_id,org_id,timestamp);
            IOrgSwitchDAO orgSwitchDAO = DAOFactory.getOrgSwitchDAO();
            orgSwitchDAO.insert(orgSwitch);
        });
    }
}
