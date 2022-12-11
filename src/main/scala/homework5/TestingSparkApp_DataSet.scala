//Основная инструкция, задание 3:
//Загрузить данные в DataSet из файла с фактическими данными поездок в Parquet (src/main/resources/data/yellow_taxi_jan_25_2018).
//С помощью DSL и lambda построить таблицу, которая покажет, как происходит распределение поездок по дистанции?
//Результат вывести на экран и записать в бд Постгрес (докер в проекте). Для записи в базу данных необходимо продумать и также приложить инит sql файл со структурой.
//(Пример: можно построить витрину со следующими колонками: общее количество поездок, среднее расстояние, среднеквадратическое отклонение, минимальное и максимальное расстояние)
//Результат: В консоли должны появиться данные с результирующей таблицей, в бд должна появиться таблица. Решение оформить в github gist.

package homework5

import com.typesafe.config.ConfigFactory
//import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.util.Properties

object TestingSparkApp_DataSet extends App {
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  import homework2.Model._
  import spark.implicits._

  def parquet2DataSet(path: String)(implicit spark: SparkSession): Dataset[TaxiRide] = {
    spark
      .read
      .parquet(path)
      .repartition(3) // number of executors * number of cores
      .persist(StorageLevel.MEMORY_ONLY)
      .as[TaxiRide]
  }

  def csv2DataSet(path: String)(implicit spark: SparkSession): Dataset[TaxiZone] = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
      .repartition(3) // number of executors * number of cores
      .persist(StorageLevel.MEMORY_ONLY)
      .as[TaxiZone]
  }

  def processTaxiData(taxiDS: Dataset[TaxiRide], taxiZoneDS: Dataset[TaxiZone]): Dataset[Row] = {
    taxiDS
      .filter(x => x.DOLocationID != 0)
      .join(broadcast(taxiZoneDS), col("PULocationID") === col("LocationID"), "left")
      .groupBy(col("Borough"))
      .agg(
        count("*").as("total_trips"),
        round(mean("trip_distance"), 2).as("mean_distance"),
        min("trip_distance").as("min_distance"),
        max("trip_distance").as("max_distance")
      )
      .orderBy(col("total_trips").desc)
  }

  def writeDS(resultDS: Dataset[Row]): Unit = {
    //val context: SparkContext = spark.sparkContext
    val config = ConfigFactory.load()
    val driver = config.getString("driver")
    val username = config.getString("username")
    val password = config.getString("password")
    val url = config.getString("url")
    val table = config.getString("table")

    val connectionProperties = new Properties()
    connectionProperties.put("user", username)
    connectionProperties.put("password", password)

    resultDS
      .write.mode(SaveMode.Overwrite).jdbc(url = url, table, connectionProperties)
  }

  val taxiDS = parquet2DataSet("src/main/resources/data/yellow_taxi_jan_25_2018")
  val taxiZoneDS = csv2DataSet("src/main/resources/data/taxi_zones.csv")
  val resultDS = processTaxiData(taxiDS,taxiZoneDS)
  writeDS(resultDS)

}

