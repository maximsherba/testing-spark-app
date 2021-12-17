package qa

import io.circe.{Decoder, parser}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Test extends App {
  implicit val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Taxi Big Data Application")
    .getOrCreate()


  case class Item(
                   sepal_length: Double,
                   sepal_width: Double,
                   petal_length: Double,
                   mixed_structure: Map[String, String],
                   petal_width: Double)

  object Item {

    //    def apply(a: Array[String]): Item =
    //      Item(
    //        a(0).toDouble,
    //        a(1).toDouble,
    //        a(2).toDouble,
    //        a(3).toDouble
    //      )
  }

  // reading
  val input_data: DataFrame = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "test")
    .option("subscribe", "science")
    .option("failOnDataLoss", false)
    .load()

  import spark.implicits._
  //  val strEncoder = Encoders.product[String]

  implicit val decodeItem: Decoder[Item] = ???
  //    deriveDecoder[Item].prepare(
  //      _.downField("sepal_length").downField("sepal_width").downField("petal_length").downField("petal_width")
  //    )


  parser.decode[Item]("") match {
    case Right(data) => ???
    case Left(exp) => ???
  }

  import com.github.plokhotnyuk.jsoniter_scala.core._
  import com.github.plokhotnyuk.jsoniter_scala.macros._

  implicit val codec: JsonValueCodec[Item] = JsonCodecMaker.make

  // transformation
  val rawDF: Dataset[Item] = input_data
    .selectExpr("CAST(value AS STRING)")
    .as[String]
    .map(readFromString[Item](_))


  // writing sink 1
  rawDF
    .writeStream
    .outputMode("append")
    .format("console")
    .start()

  // writing sink 2
  rawDF
    .writeStream
    .outputMode("append")
    //.format("console")
    .format("kafka")
    .option("kafka.bootstrap.servers", "BootstrapServers")
    .option("failOnDataLoss", false)
    .option("topic", "outputTopic")
    .start()
}
