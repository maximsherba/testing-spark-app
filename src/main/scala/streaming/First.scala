package streaming

import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

import scala.concurrent.duration.DurationInt


object First extends App {
  val spark = SparkSession.builder()
    .appName("Integrating Kafka")
    .master("local[2]")
    .getOrCreate()

  val schema = StructType(Array(
    StructField("timestamp", StringType),
    StructField("page", StringType)
  ))

  def readFromKafka(): DataFrame = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "input")
    .load()


//  def transform(in: DataFrame): DataFrame = in
//    .select(expr("cast(value as string) as actualValue"))
//    .select(from_json(col("actualValue"), schema).as("page")) // composite column (struct)
//    .selectExpr("page.timestamp as timestamp", "page.page as page")
//    .select(date_format(to_timestamp(col("timestamp"), "dd-MM-yyyy HH:mm:ss:SSS"), "HH:mm:ss:SSS").as("time"), col("page"))


  case class Record(timestampType: TimestampType, pageType: String)

  import spark.implicits._

  def transformToCaseClass(in: DataFrame): Dataset[String] =
    in
      .select(expr("cast(value as string) as actualValue"))
      .as[String]
//      .map{ value => }


  def writeDfToCassandra(in: DataFrame): Unit =
    in.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .start()
      .awaitTermination()

  def writeDsToCassandra(in: Dataset[String]): Unit =
    in.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .start()
      .awaitTermination()


  val frame = readFromKafka()

  val transformed = transformToCaseClass(frame)

  writeDsToCassandra(transformed)
}