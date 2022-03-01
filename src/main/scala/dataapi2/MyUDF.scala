package dataapi2

import dataapi2.SparkSql.taxiZonesDF
import lesson2.OtusMethodsForTest.{readCSV, readParquet}
import org.apache.spark.sql.SparkSession

object MyUDF extends App {
  implicit val spark = SparkSession
    .builder()
    .config("spark.master", "local")
    .appName("Taxi Big Data Application")
    .getOrCreate()

  val taxiZonesDF = readCSV("src/main/resources/data/taxi_zones.csv")

  taxiZonesDF.show()

  val my_udf = (s: String) => {
    if (s != null)
      s.length
    else 0}

  spark.udf.register("strlen", my_udf)

  taxiZonesDF.createOrReplaceTempView("taxi_zones")

  val filteredTaxiZonesDF = spark.sql(
    """
      |select Borough, Zone, strlen(Zone)
      |from taxi_zones
      |where Borough = 'Queens'
      |and Borough is not null
      |and strlen(Zone) > 15
    """.stripMargin)

  filteredTaxiZonesDF.show()


}
