package session

import org.apache.spark.sql.SparkSession

object BasicOperation {
  case class Employee(name: String, age: Long, depId: Long, gender: String, salary: Long)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(BasicOperation.getClass.getName)
      .master("local")
      .config("spark.sql.warehouse.dir","d:/spark-warehouse")
      .getOrCreate()
    import spark.implicits._
    val employee = spark.read.json("data\\depart\\employee.json")
    employee.cache()
    employee.createOrReplaceTempView("employee")
    spark.sql("select * from employee where age > 30").show()
    employee.printSchema()
    val employeeWithAgeGreaterThen30DF = spark.sql("select * from employee where age > 30")
    employeeWithAgeGreaterThen30DF.write.json("d:/writetest.json")
    val employeeDS = employee.as[Employee]
    employeeDS.show()
    employeeDS.printSchema()
    val employeeDF = employeeDS.toDF()
  }
}
