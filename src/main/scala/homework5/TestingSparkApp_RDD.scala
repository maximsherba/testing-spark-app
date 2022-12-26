//Основная инструкция, задание 2:
//Сформировать ожидаемый результат и покрыть простым тестом (с библиотекой AnyFlatSpec) витрину из домашнего задания к занятию Spark Data API,
//построенную с помощью RDD. Пример src/test/scala/lesson2/SimpleUnitTest.scala
package homework5

import homework2.Model._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel._

//С помощью lambda построить таблицу, которая покажет в какое время происходит больше всего вызовов
object TestingSparkApp_RDD extends App {
  implicit val spark = SparkSession.builder()
    .appName("Testing")
    .config("spark.master", "local[2]")
    .getOrCreate()

  import spark.implicits._

  def Parquet2RDD(path: String)(implicit spark: SparkSession): RDD[TaxiRide] = {
    spark.read
      .load(path)
      //.select("") if we need projection of columns
      .repartition(3)
      .persist(MEMORY_AND_DISK)
      .as[TaxiRide]
      .rdd
  }

  def processTaxiRideRDD(rdd: RDD[TaxiRide]): RDD[String] = rdd
    .map {
      line => (line.tpep_pickup_datetime.toString().substring(11, 13), 1)
    }
    .reduceByKey(_ + _)
    .sortBy(_._2, false)
    .map(tup => s"${tup._1} ${tup._2}")

  val taxiFactsRDD = Parquet2RDD("src/main/resources/data/yellow_taxi_jan_25_2018")

  val mappedTaxiFactRDD = processTaxiRideRDD(taxiFactsRDD)

  mappedTaxiFactRDD.foreach(x => println(x))
  mappedTaxiFactRDD.coalesce(1).saveAsTextFile("src/main/resources/data/result")

  println(mappedTaxiFactRDD.collect().toString())
}

