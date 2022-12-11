//If you don’t have winutils.exe installed,
//please download the wintils.exe and hadoop.dll files from https://github.com/steveloughran/winutils
//Copy the downloaded files to a folder on your system, for example,
//let’s say you have copied these files to c:/hadoop/bin, set the below environment variables.
//set HADOOP_HOME=c:/hadoop
//set PATH=%PATH%;%HADOOP_HOME%/bin;
//Close and reload the command line or terminal to initialize these variables.
//Sometimes you may also need to put hadoop.dll file into the C:/Windows/System32 folder.

//With cache(), you use only the default storage level :
//MEMORY_ONLY for RDD
//MEMORY_AND_DISK for Dataset
//Use persist() if you want to assign another storage level

//Основная инструкция, задание 1:
//Загрузить данные в первый DataFrame из файла с фактическими данными поездок в Parquet (src/main/resources/data/yellow_taxi_jan_25_2018).
//Загрузить данные во второй DataFrame из файла со справочными данными поездок в csv (src/main/resources/data/taxi_zones.csv).
//С помощью DSL построить таблицу, которая покажет какие районы самые популярные для заказов. Результат вывести на экран и записать в файл Паркет.
//Результат: В консоли должны появиться данные с результирующей таблицей, в файловой системе должен появиться файл. Решение оформить в github gist.

package homework2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel._

object DataApiHomeWorkTaxi_DataFrame extends App {
  def readParquet(path: String)(implicit spark: SparkSession): DataFrame =
    spark.read
      .load(path)
      .repartition(3)
      .persist(MEMORY_AND_DISK)

  def readCSV(path: String)(implicit spark: SparkSession): DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
      .repartition(3) // number of executors * number of cores
      .persist(MEMORY_AND_DISK)

  def processTaxiData(taxiDF: DataFrame, taxiZonesDF: DataFrame) = {
    taxiDF
      .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
      .groupBy("Borough")
      .agg(
        count("*").as("total_trips"),
      )
      .orderBy(col("total_trips").desc)
  }

  implicit val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  //val allData: DataFrame = spark.read.parquet("hdfs://master:9000/data/release/ods/session/bdp_day=20191126").cache()
  val taxiFactsDF = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")
  taxiFactsDF.printSchema()
  println(taxiFactsDF.count())

  val taxiZonesDF = readCSV("src/main/resources/data/taxi_zones.csv")
  //taxiZonesDF.show()

  val resultDF = processTaxiData(taxiFactsDF, taxiZonesDF)
  resultDF.show()

  // Запись в корень проекта
  //result1DF.write.mode(SaveMode.Overwrite).parquet("./results/nationalNames-pq")
  resultDF.coalesce(1).write.mode(SaveMode.Overwrite).parquet("src/main/resources/results/task1/nationalNames-pq")

}

