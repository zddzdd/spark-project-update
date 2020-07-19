package com.micro.bigdata.spark.login

import com.micro.bigdata.utils.{DateUtils, StringUtils}
import org.apache.spark.{SparkConf, SparkContext}

object LoginAnalyse {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(LoginAnalyse.getClass.getName)
      .setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("data/access2.log")

    val rdd = lines.map(line => {
      var date:String = ""
      var event_name = ""
      var uid = ""
      var p_c = ""
      try {
        val params = line.split("\\?")(1);
        event_name = StringUtils.getFieldFromConcatString(params, "&", "event_name");
        uid = StringUtils.getFieldFromConcatString(params, "&", "uid");
        val t = StringUtils.getFieldFromConcatString(params, "&", "t");
        p_c = StringUtils.getFieldFromConcatString(params, "&", "p_c");
        val datekey = (t.toLong/1000).toString
        date = DateUtils.timeStamp2Date(datekey,"yyyy-MM-dd")
      }catch {
        case e:Exception=>{
          println("line:"+line)
        }
      }
      (date,(event_name,uid,p_c))

    });
    val filter_rdd = rdd.filter(_._2._1.equals("login_org"))
    val result = filter_rdd.groupByKey().map(kv=>{
      val key = kv._1
      val value = kv._2.map(line=>{
        (line._3,line._2)
      }).groupBy(_._1).map(line=>{
        val pc = line._1
        val uid = line._2.map(_._2).toList.distinct.size
        (pc,uid)
      })
      (key,value)
    })
    result.foreach(println)
  }
}
