package com.micro.bigdata.spark.login

import java.sql.{Date, Timestamp}

import com.micro.bigdata.dao.factory.DAOFactory
import com.micro.bigdata.domain.LoginDetail
import com.micro.bigdata.utils.{DateUtils, StringUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object OrgLoginCount {
  case class CountAgg(date:String,orgid:String,pc_code:String)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(LoginAnalyse.getClass.getName)
      .setMaster("local")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir","d:/spark-warehouse")
      .getOrCreate()

    import spark.implicits._
    val lines = sc.textFile("data/access2.log")
    val rdd = lines.map(line=>{
      var org_id = ""
      var date:String = ""
      var event_name = ""
      var uid = ""
      var p_c = ""
      var login_time= ""
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
      (date,uid,org_id,event_name,p_c)
    })

    rdd.groupBy(_._1).flatMap(line=>{
      val date = line._1
      val userinfos = line._2.map(line=>{
        val uid = line._2
        val event_name = line._4
        val pc = line._5
        (uid,(event_name,pc))
      })
      val map = userinfos.groupBy(_._1).flatMap(kv=>{
        val uid = kv._1
        var index =0
        kv._2.foreach(line=>{
          val event = line._2._1
          if(event.equals("exchange_org")) index +=1
        })
        if(index == 0){
          kv._2.map(line=>{
            val pc = line._2._2
            pc
          })
        }else{
          "a"
        }
      }).map(date+"_"+_)
      map
    }).map((_,1)).reduceByKey(_+_).foreach(println)

    /*val filter_rdd = rdd.filter(line=>{
      line._4.equals("exchange_org")
    })
    val countRdd = filter_rdd.map(line=>{
      CountAgg(line._1,line._3,line._5)
    })
    val countDS = countRdd.toDS()
    countDS.registerTempTable("temp_switch")
    val result1df = spark.sql("select date,orgid,pc_code, count(0) from temp_switch group by date,orgid,pc_code")
  */
  }
}
