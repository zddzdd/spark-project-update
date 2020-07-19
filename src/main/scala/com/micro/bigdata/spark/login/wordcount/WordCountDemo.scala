package com.micro.bigdata.spark.login.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCountDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(WordCountDemo.getClass.getName)
      .setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("data/hello.txt")
    val map = lines.flatMap(line=>{
      line.split(" ")
    }).map((_,1))
    val result = map.reduceByKey(_+_)
    result.foreach(println)
  }

}
