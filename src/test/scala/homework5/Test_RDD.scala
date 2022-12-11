package homework5

import homework5.TestingSparkApp_RDD.{Parquet2RDD, processTaxiRideRDD}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class Test_RDD extends AnyFlatSpec {

  implicit val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Test RDD")
    .getOrCreate()

  it should "upload and process data" in {
    val taxiFactsRDD = Parquet2RDD("src/main/resources/data/yellow_taxi_jan_25_2018")
    val mappedTaxiFactRDD = processTaxiRideRDD(taxiFactsRDD)

    //val actualDistribution = mappedTaxiFactRDD.foreach(x => println(x))
      //.collectAsList()
      //.get(0)

    //assert(actualDistribution == 6)
  }

}
