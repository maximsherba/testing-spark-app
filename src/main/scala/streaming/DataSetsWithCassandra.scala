package streaming

import com.github.plokhotnyuk.jsoniter_scala.core.{JsonValueCodec, readFromString}
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import org.apache.spark.sql.cassandra.DataFrameWriterWrapper
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{DataStreamWriter, GroupState, GroupStateTimeout, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

object DataSetsWithCassandra extends App {
  val parallelism = 4

  val spark = SparkSession
    .builder()
    .appName("Integrating Kafka")
    .master(s"local[$parallelism]")
    .config("spark.cassandra.connection.host", "127.0.0.1")
    .config("spark.cassandra.connection.port", "9042")
    .config("spark.cassandra.output.consistency.level", "LOCAL_ONE")
    .getOrCreate()

  val schema = StructType(
    Array(StructField("timestamp", StringType), StructField("page", StringType))
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
      .select(from_json(col("actualValue"), schema).as("page"))
      .selectExpr("page.timestamp as timestamp", "page.page as page")
      .select(
        date_format(to_timestamp(col("timestamp"), "dd-MM-yyyy HH:mm:ss:SSS"), "HH:mm:ss:SSS")
          .as("time"),
        col("page")
      )


  import spark.implicits._
  implicit val codec: JsonValueCodec[ClickRecord] = JsonCodecMaker.make[ClickRecord]

  case class ClickRecord(timestamp: String, page: String, userId: Long, duration: Int)

  def transformToCaseClass(in: DataFrame): Dataset[ClickRecord] =
    in
      .select(expr("cast(value as string) as actualValue"))
      .as[String]
      .map(readFromString[ClickRecord](_))

  case class ClickBulkState(pageType: String, count: Int, totalDuration: Int)
  case class MapResultClickAverage(page: String, averageDuration: Double)

  def updateCountState(
      key: String,
      group: Iterator[ClickRecord],
      state: GroupState[ClickBulkState]
  ): MapResultClickAverage = {
    val prevState = if (state.exists) state.get else ClickBulkState(key, 0, 0)

    val totalAggData = group.foldLeft((0, 0)) {
      case (acc, rec) =>
        val (count, duration) = acc
        (count + 1, duration + rec.duration)
    }

    val (totalCount, totalDuration) = totalAggData
    val newPostBulk = ClickBulkState(key, prevState.count + totalCount, prevState.totalDuration + totalDuration)

    state.update(newPostBulk)
    MapResultClickAverage(key, newPostBulk.totalDuration * 1.0 / newPostBulk.count)
  }


  val rawInputDF = readFromKafka()

  val transformedDS = transformToCaseClass(rawInputDF)

  val averageClicksDS: Dataset[MapResultClickAverage] = transformedDS
    .groupByKey(_.page)
    .mapGroupsWithState(GroupStateTimeout.NoTimeout())(updateCountState)


  transformedDS.writeStream
    .foreachBatch { (batch: Dataset[ClickRecord], _: Long) =>
      batch
        .select($"userId", $"page", $"duration", $"timestamp")
        .write
        .cassandraFormat("click", "public")
        .mode(SaveMode.Append)
        .save()
    }
    .start()
    .awaitTermination()

}
