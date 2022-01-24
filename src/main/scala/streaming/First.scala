package streaming

import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.concurrent.duration.DurationInt


object First extends App {

  val spark = SparkSession.builder()
    .appName("Integrating Kafka")
    .master("local[2]")
    .getOrCreate()

  val schema = StructType(Array(
    //    StructField("timestamp", TimestampType),
    StructField("timestamp", StringType),
    StructField("page", StringType)
  ))

  def readFromKafka(): DataFrame = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "input")
    .load()



  def tranform(in: DataFrame): DataFrame = in
    .select(expr("cast(value as string) as actualValue"))
    .select(from_json(col("actualValue"), schema).as("page")) // composite column (struct)
    .selectExpr("page.timestamp as timestamp", "page.page as page")
    .withColumn("quantity", lit(1))
    .withColumn("tmst", to_timestamp(col("timestamp"), "dd-MM-yyyy HH:mm:ss:SSS"))


  //    .filter(col("page") === "/jobs")
  //    .groupBy(col("page"))
  //    .count()


  def writeCassandra(in: DataFrame): Unit =
    in.writeStream
      .format("console")
      .outputMode("complete")
      //      .outputMode("append")
      //      .trigger(
      //        Trigger.ProcessingTime(2.seconds)
      ////        Trigger.Continuous(2.seconds)
      //      )
      .start()
      .awaitTermination()

  val frame = readFromKafka()

  val transformed: DataFrame = tranform(frame)

  val windowByDay = transformed
    .groupBy(window(col("tmst"), "10 seconds").as("time")) // tumbling window: sliding duration == window duration
    .agg(sum("quantity").as("totalQuantity"))
    .select(
      col("time").getField("start").as("start"),
      col("time").getField("end").as("end"),
      col("totalQuantity")
    )

  println(transformed.isStreaming)

  writeCassandra(windowByDay)


}