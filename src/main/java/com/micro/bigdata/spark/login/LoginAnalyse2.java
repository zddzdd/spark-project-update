package com.micro.bigdata.spark.login;

import com.micro.bigdata.domain.UserLogin;
import com.micro.bigdata.utils.DateUtils;
import com.micro.bigdata.utils.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple4;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

public class LoginAnalyse2 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(LoginAnalyse2.class.getName())
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("d:/access.log");
        JavaPairRDD<String, Tuple4> pairRDD = lines.mapToPair(line -> {
            String params = line.split("\\?")[1];
            String event_name = StringUtils.getFieldFromConcatString(params, "&", "event_name");
            String org_id = StringUtils.getFieldFromConcatString(params, "&", "org_id");
            String uid = StringUtils.getFieldFromConcatString(params, "&", "uid");
            String t = StringUtils.getFieldFromConcatString(params, "&", "t");
            String p_c = StringUtils.getFieldFromConcatString(params, "&", "p_c");
            String datekey = String.valueOf(Long.valueOf(t) / 1000);
            return new Tuple2<>(org_id, new Tuple4(datekey, uid, event_name, p_c));
        });

        JavaPairRDD<String, Iterable<Tuple4>> org_detail = pairRDD.groupByKey();
        org_detail.foreach(x-> System.out.println(x));


       /*JavaPairRDD<String,Tuple2> pairRDD = map.mapToPair(line->{
            String datekey = String.valueOf(Long.valueOf(line._1())/1000);
            String userid = line._2();
            String org_id = line._3();
            return new Tuple2<>(org_id,new Tuple2(datekey,userid));
        });

        JavaPairRDD<String, Iterable<Tuple2>> org_detail = pairRDD.groupByKey();
        org_detail.foreach(x-> System.out.println(x));
        // 统计企业被登录用户明细
//        org_login_detail(org_detail);

*/
        // 统计企业被登录用户数
        org_login_count(org_detail);
    }

    private static void org_login_detail(JavaPairRDD<String, Iterable<Tuple2>> org_detail) {
        org_detail.foreach(line->{
            String org_id = line._1;
            Iterator<Tuple2> iterator = line._2.iterator();
            String temp_date = "";
            ArrayList<UserLogin> list = new ArrayList<>();
            while (iterator.hasNext()){
                Tuple2<String,String> next = iterator.next();
                String datekey = next._1;
                String date = DateUtils.timeStamp2Date(datekey, "yyyy-MM-dd");
                String userid = next._2;
                Long login_time = Long.valueOf(datekey);
                UserLogin userLogin = new UserLogin(login_time, userid);
                if(temp_date.equals("")){
                    temp_date = date;
                    list.add(userLogin);
                }else {
                    if(date.equals(temp_date)){
                        int uindex = 0;
                        for (int i = 0; i < list.size(); i++) {
                            UserLogin ori_userLogin = list.get(i);
                            if(ori_userLogin.getUser_id().equals(userid)){
                                uindex+=1;
                                Long or_login_time = userLogin.getLogin_time();
                                if(or_login_time<login_time){
                                    list.set(i,userLogin);
                                }
                            }
                        }
                        if(uindex==0){
                            list.add(userLogin);
                        }
                    }else {
                        System.out.println(org_id+"---"+temp_date+"---"+list.toString());
                        list.clear();
                        list.add(new UserLogin(Long.valueOf(datekey),userid));
                        temp_date = date;
                    }
                }
            }
            if(list.size()!=0){
                System.out.println(org_id+"---"+temp_date+"---"+list.toString());
            }
        });
    }

    //Tuple4(datekey, uid, event_name, p_c)
    private static void org_login_count(JavaPairRDD<String, Iterable<Tuple4>> org_detail) {

        org_detail.foreach(line->{
            Iterator<Tuple4> iterator = line._2.iterator();
            String temp_date = "";
            String temp_pc = "";
            HashSet<String> set = new HashSet<>();
            while (iterator.hasNext()){
                Tuple4<String,String,String,String> next = iterator.next();
                String datekey = next._1();
                String date = DateUtils.timeStamp2Date(datekey, "yyyy-MM-dd");
                String userid = next._2();
                String pc_code = next._4();

                if(temp_date.equals("")){
                    temp_date = date;

                    set.add(userid);
                }else {
                    if(date.equals(temp_date)){
                        set.add(userid);
                    }else {
                        System.out.println(temp_date+"---"+set.size());
                        set.clear();
                        set.add(userid);
                        temp_date = date;
                    }
                }
            }
            if(set.size()!=0){
                System.out.println(temp_date+"---"+set.size());
            }
        });
    }
}
