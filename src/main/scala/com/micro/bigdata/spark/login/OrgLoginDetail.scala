package com.micro.bigdata.spark.login

import java.sql.{Date, Timestamp}

import com.micro.bigdata.dao.factory.DAOFactory
import com.micro.bigdata.domain.LoginDetail
import com.micro.bigdata.utils.{DateUtils, StringUtils}
import org.apache.spark.{SparkConf, SparkContext}

object OrgLoginDetail {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(LoginAnalyse.getClass.getName)
      .setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("data/access2.log")
    val rdd = lines.map(line=>{
      var org_id = ""
      var date:String = ""
      var event_name = ""
      var uid = ""
      var p_c = ""
      var login_time=""
      var datekey = ""
        try {
        val params = line.split("\\?")(1)
        org_id = StringUtils.getFieldFromConcatString(params,"&","org_id")
        event_name = StringUtils.getFieldFromConcatString(params, "&", "event_name")
        uid = StringUtils.getFieldFromConcatString(params, "&", "uid")
        val t = StringUtils.getFieldFromConcatString(params, "&", "t")
        p_c = StringUtils.getFieldFromConcatString(params, "&", "p_c")
        datekey = (t.toLong/1000).toString
        date = DateUtils.timeStamp2Date(datekey,"yyyy-MM-dd")
      }catch {
        case e:Exception=>{
          println("line:"+line)
        }
      }
      (date,(uid,org_id,event_name,p_c,datekey))
    })

    val result = rdd.groupByKey().map(line=>{
      val date = line._1
      val infordd = line._2.map(info=>{
        (info._1,(info._2,info._3,info._4,info._5))
      }).groupBy(_._1).map(kv=>{
        val userid = kv._1
        var temp_time = 0L
        var org_id = ""
        var pc_code = ""
        kv._2.map(line=>{
          val event_name = line._2._2
          if(event_name.equals("exchange_org")){
            org_id = line._2._1
            pc_code = line._2._3
            val exchange_time = line._2._4.toLong
            if(exchange_time>temp_time) temp_time = exchange_time
          }else{
            if(pc_code.equals("")) pc_code = line._2._3
          }
        })
//        (userid,org_id,temp_time,pc_code)
        (org_id,pc_code,userid,temp_time)
      })
      (date,infordd)
    })
//    result.foreach(println)
    result.foreach(x=>{
      val datekey = x._1
      x._2.foreach(info=>{
        val org_id = info._1
        val pc_code = info._2
        val userid = info._3
        val login_time = info._4.toLong
        val date = new Date(login_time);
        val timestamp = new Timestamp(date.getTime());
        val loginDetail = new LoginDetail(datekey,org_id,pc_code,userid,timestamp)
        val loginDetailDao = DAOFactory.getLoginDetailDAO()
        loginDetailDao.insert(loginDetail)
      })
    })
  }
}
