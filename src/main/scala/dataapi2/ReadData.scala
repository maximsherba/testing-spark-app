package dataapi2

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.util.Properties

object ReadData extends App {
  implicit val spark = SparkSession
    .builder()
    .config("spark.master", "local")
    .appName("Taxi Big Data Application")
    .getOrCreate()

  import spark.implicits._

  case class Result(last_name: String, title: String)

  val query: String = """(
                         |select e.last_name, t.title
                         |  from employees e
                         |    join titles t on e.emp_no = t.emp_no
                         |  order by e.last_name
                         |) as result
                         |""".stripMargin

  val connProps = new Properties()
  connProps.put("user", "docker")
  connProps.put("password", "docker")

  val resultDS: Dataset[Result] =
    spark.read
    .jdbc("jdbc:postgresql://localhost:5432/otus", query, connProps)
    .as[Result]

//  val resultDS =
//  spark
//    .read
//    .format("jdbc")
//    .option("url", "jdbc:postgresql://localhost:5432/otus")
//    .option("user", "docker")
//    .option("password", "docker")
//    .option("dbtable", query)
////    .option("dbtable", "employees")
//    .load()
//    .as[Result]

  resultDS.show()

}
