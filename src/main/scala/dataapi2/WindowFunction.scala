package dataapi2

import dataapi2.MyUDF.taxiZonesDF
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.{Window, WindowSpec}

object WindowFunction extends App {
  implicit val spark = SparkSession
    .builder()
    .config("spark.master", "local")
    .appName("Taxi Big Data Application")
    .getOrCreate()

  val simpleData = Seq(("James", "Sales", 3000), ("John", "ServiceDesk", 4600), ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000), ("James", "Sales", 3000), ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900), ("Jeff", "Marketing", 3000), ("Kumar", "Marketing", 2000), ("Saif", "Sales", 4100)
  )

  val employeeDF = spark.createDataFrame(simpleData).toDF("employee_name", "department", "salary")
//  employeeDF.show()

  employeeDF.createOrReplaceTempView("employee")

  val resultDF: DataFrame = spark.sql("""
     |select
     |  employee_name,
     |  department,
     |  salary,
     |  dense_rank() OVER (PARTITION BY department ORDER BY salary DESC) as rank
     |from employee
    """.stripMargin)

//  resultDF.show()

  import org.apache.spark.sql.functions._

  val windowSpec: WindowSpec = Window.partitionBy("department").orderBy(col("salary").desc)

  employeeDF
    //    .withColumn("rank", rank().over(windowSpec))
    .withColumn("rank", dense_rank().over(windowSpec))
    .show()

  /**
   * 1. Find second salary 4100
   *
   * */

}
