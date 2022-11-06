package streaming

import com.github.plokhotnyuk.jsoniter_scala.core.{JsonValueCodec, readFromString}
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{DataStreamWriter, GroupState, GroupStateTimeout, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

import scala.concurrent.duration.DurationInt

object DataSets extends App {

  val spark = SparkSession
    .builder()
    .appName("Integrating Kafka")
    .master("local")
    .getOrCreate()

  val schema = StructType(
    Array(
      StructField("timestamp", StringType),
      StructField("page", StringType)
    )
  )

  def readFromKafka(): DataFrame =
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:29092")
      .option("subscribe", "input")
      .load()

  def transform(in: DataFrame): DataFrame =
    in
      .select(expr("cast(value as string) as actualValue"))
      .select(from_json(col("actualValue"), schema).as("page")) // composite column (struct)
      .selectExpr("page.timestamp as timestamp", "page.page as page")
      .select(
        date_format(to_timestamp(col("timestamp"), "dd-MM-yyyy HH:mm:ss:SSS"), "HH:mm:ss:SSS")
          .as("time"),
        col("page")
      )

  case class ClickRecord(timestamp: String, page: String, userId: Long, duration: Int)
  case class ClickBulk(pageType: String, count: Int, totalDuration: Int)
  case class ClickAverage(page: String, averageDuration: Double)

  import spark.implicits._

  implicit val codec: JsonValueCodec[ClickRecord] = JsonCodecMaker.make[ClickRecord]

  def transformToCaseClass(in: DataFrame) =
    in
      .select(expr("cast(value as string) as actualValue"))
      .as[String]
      .map(readFromString[ClickRecord](_))

  def updateCountState( // вызывается один раз для каждой группы в микробатче
                        postKey: String,
                        group: Iterator[ClickRecord],
                        state: GroupState[ClickBulk] // не мб null IllegalArgumentException
                      ): ClickAverage = { // на выходе только одна запись
    val prevState = if (state.exists) state.get else ClickBulk(postKey, 0, 0)

    val totalAggData = group.foldLeft((0, 0)) {
      case (acc, rec) =>
        val (count, duration) = acc
        (count + 1, duration + rec.duration)
    }

    val (totalCount, totalDuration) = totalAggData
    val newPostBulk =
      ClickBulk(postKey, prevState.count + totalCount, prevState.totalDuration + totalDuration)

    state.update(newPostBulk) // потокобезопасная
    ClickAverage(postKey, newPostBulk.totalDuration * 1.0 / newPostBulk.count)
  }

  val frame = readFromKafka()

  val transformed: Dataset[ClickRecord] = transformToCaseClass(frame)

  val transformed2 = transform(frame)

    println(transformed2.isStreaming)

//    transformed2.writeStream
//      .trigger(Trigger.ProcessingTime(2.seconds))
//      .option("checkpointLocation", "src/main/resources/data/checkpoints")
//      .format("parquet")
//      .start("src/main/resources/data/out")
//      .awaitTermination()

//    transformed
//          .writeStream
//          .outputMode("update")
//          .foreachBatch { (batch: Dataset[ClickRecord], _: Long) =>
//            batch.show()
//            batch.isStreaming // false
//            batch.write
//          }
//          .trigger(Trigger.Once())
//          .start()
//          .awaitTermination()
//
//  transformed
//      .writeStream
//      .format("console")
//      .outputMode("append")
//      .trigger(Trigger.Once())
//      .start()
//      .awaitTermination()

  //
//  val averageDS: Dataset[ClickAverage] = transformed
//    .groupByKey(_.page)
//    .mapGroupsWithState(GroupStateTimeout.NoTimeout())(updateCountState)
//
//  averageDS
//    .writeStream
//    .outputMode("update")
//    .foreachBatch { (batch: Dataset[ClickAverage], _: Long) =>
//      batch.show()
//    }
//    .trigger(Trigger.ProcessingTime(1.seconds))
//    .start()
//    .awaitTermination()

  /**
   * Batch1
   *  +------+------------------+
   * |  page|   averageDuration|
   * +------+------------------+
   * |/about|105.73825503355705|
   * | /shop| 91.11333333333333|
   * | /news|102.57046979865771|
   * |/index| 98.02666666666667|
   * | /jobs|111.31333333333333|
   * | /help|101.02013422818791|
   * +------+------------------+
   *
   * Batch2 about + shop + help
   *  +------+------------------+
   * |  page|   averageDuration|
   * +------+------------------+
   * |/about|105.73825503355705|
   * | /shop| **** 91.11333333333333|
   * | /about|*** 102.57046979865771|
   * |/index| 98.02666666666667|
   * | /jobs|111.31333333333333|
   * | /help| *** 101.02013422818791|
   * +------+------------------+
   *
   *
   *
   * */
}
