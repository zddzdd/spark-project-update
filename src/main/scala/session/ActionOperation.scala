package session

import org.apache.spark.sql.SparkSession

object ActionOperation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(ActionOperation.getClass.getName)
      .master("local")
      .config("spark.sql.warehouse.dir","d:/spark-warehouse")
      .getOrCreate()
    import spark.implicits._
    val employee = spark.read.json("data\\depart\\employee.json")
    employee.collect().foreach(println)
    println(employee.count())
    println(employee.first())
    employee.foreach{println(_)}
    println(employee.map(employee=>1).reduce(_+_))
    employee.show()
    employee.take(3).foreach(println(_))
  }
}
