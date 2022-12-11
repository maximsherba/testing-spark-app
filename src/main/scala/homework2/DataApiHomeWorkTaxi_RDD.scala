package homework2

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel._

//С помощью lambda построить таблицу, которая покажет в какое время происходит больше всего вызовов
object DataApiHomeWorkTaxi_RDD extends App {
  val spark: SparkSession = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local[2]")
    .getOrCreate()

  val context: SparkContext = spark.sparkContext

  import Model._
  import spark.implicits._

  val taxiFactsRDD: RDD[TaxiRide] = spark.read
      .load("src/main/resources/data/yellow_taxi_jan_25_2018")
      //.select("") if we need projection of columns
      .repartition(3)
      .persist(MEMORY_AND_DISK)
      .as[TaxiRide]
      .rdd

  val mappedTaxiFactRDD =
    taxiFactsRDD
      .map {
        line => (line.tpep_pickup_datetime.toString().substring(11,13), 1)
      }
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .map(tup => s"${tup._1} ${tup._2}")

  mappedTaxiFactRDD.foreach(x => println(x))
  mappedTaxiFactRDD.saveAsTextFile("src/main/resources/data/result")
}

