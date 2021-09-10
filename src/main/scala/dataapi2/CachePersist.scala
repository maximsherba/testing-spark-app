package dataapi2

import lesson2.OtusMethodsForTest.{readCSV, readParquet}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel

object CachePersist extends App {
  implicit val spark = SparkSession
    .builder()
    .config("spark.master", "local")
    .appName("Taxi Big Data Application")
    .getOrCreate()

  val taxiFactsDF = readParquet("ftp:/config/ src/main/resources/data/yellow_taxi_jan_25_2018")

//  var file = sc.textFile("ftp://user:pwd/192.168.1.5/brecht-d-m/map/input.nt")
//  var fileDF = file.toDF()
//  fileDF.write.parquet("out")

  taxiFactsDF.show()

  val cashedCarsDF = taxiFactsDF
    .select(col("VendorID"), col("total_amount"))
    .filter(col("total_amount") > 10)
    .cache()
//    .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

  taxiFactsDF
    .filter(col("total_amount") > 10)
    .select(col("VendorID"), col("total_amount"))
    .show()

  taxiFactsDF
    .select(col("VendorID"), col("total_amount"))
    .filter(col("total_amount") > 10)
    .show()

  taxiFactsDF
    .select(col("VendorID"), col("total_amount"))
    .filter(col("total_amount") > 4)
    .show()

  cashedCarsDF.unpersist()

  spark.sql(
    """
      | CACHE TABLE testCarCache OPTIONS ('storageLevel' 'DISK_ONLY') SELECT * FROM cars;
      |""".stripMargin).show()

  spark.sql(
    """
      | select * from cars limit 3;
      |""".stripMargin).show()



}
