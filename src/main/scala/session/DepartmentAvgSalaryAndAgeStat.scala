package session

import org.apache.spark.sql.SparkSession

object DepartmentAvgSalaryAndAgeStat {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(DepartmentAvgSalaryAndAgeStat.getClass.getName)
      .master("local")
      .config("spark.sql.warehouse.dir","d:/")
      .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val employee = spark.read.json("data\\depart\\employee.json")
    val department = spark.read.json("data\\depart\\department.json")
    employee.filter("age > 20").join(department, $"depId" === $"id")
      .groupBy(department("name"),employee("gender"))
      .agg(avg(employee("salary")),avg(employee("age")))
      .show()
  }
}
