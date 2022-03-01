package dataapi2

import lesson2.OtusMethodsForTest.{readCSV, readParquet}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkSql extends App {
  implicit val spark = SparkSession
    .builder()
    .config("spark.master", "local")
    .appName("Taxi Big Data Application")
    .getOrCreate()

  val taxiZonesDF = readCSV("src/main/resources/data/taxi_zones.csv")
  val taxiFactsDF = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")

//  taxiZonesDF.createOrReplaceTempView("taxi_zones")
//  taxiFactsDF.createOrReplaceTempView("taxi_facts")

  val taxiDistanceDF = spark.sql("""
      |select
      |  tz.Borough,
      |  count(*) as `total trips`,
      |  min(trip_distance) as `min distance`,
      |  round(avg(trip_distance),2) as `avg distance`,
      |  max(trip_distance) as `max distance`
      |from taxi_facts tf
      |  left join taxi_zones tz
      |   on tz.LocationID = tf.PULocationID
      |group by tz.Borough
      |order by `total trips` desc
    """.stripMargin)

  taxiDistanceDF.show()

//  val taxiZonesDF2: DataFrame = spark.read.table("taxi_zones")
//  val taxiFactsDF2: DataFrame = spark.read.table("taxi_facts")

//  taxiZonesDF2.show()
//  taxiFactsDF2.show()

//  taxiZonesDF
//      .write
//      .saveAsTable("taxi_zones")

}