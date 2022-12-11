//Сформировать ожидаемый результат и покрыть Spark тестом (с библиотекой SharedSparkSession) витрину из домашнего задания
//к занятию Spark Data API, построенную с помощью DF и DS.
//Пример src/test/scala/lesson2/TestSharedSparkSession.scala

package homework5

import homework5.TestingSparkApp_DataSet.{csv2DataSet, parquet2DataSet, processTaxiData}
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.SharedSparkSession

class Test_DS extends SharedSparkSession {
  //import testImplicits._

  test("homework5 - DS") {
    val taxiDS = parquet2DataSet("src/main/resources/data/yellow_taxi_jan_25_2018")
    val taxiZoneDS = csv2DataSet("src/main/resources/data/taxi_zones.csv")
    val resultDS = processTaxiData(taxiDS, taxiZoneDS)

    checkAnswer(
      resultDS,
        Row("Manhattan", 30426, 0.0, 2.23, 66.0) ::
        Row("Queens", 17712, 0.0, 11.14, 53.5) ::
        Row("Unknown", 6644, 0.0, 2.34, 42.8) ::
        Row("Brooklyn", 3037, 0.0, 3.28, 27.37) ::
        Row("Bronx", 211, 0.0, 2.99, 20.09) ::
        Row("EWR", 19, 0.0, 3.46, 17.3) ::
        Row("Staten Island", 4, 0.0, 0.2, 0.5) :: Nil
    )
  }
}

