//Сформировать ожидаемый результат и покрыть Spark тестом (с библиотекой SharedSparkSession) витрину из домашнего задания
//к занятию Spark Data API, построенную с помощью DF и DS.
//Пример src/test/scala/lesson2/TestSharedSparkSession.scala

package homework5

import homework2.DataApiHomeWorkTaxi_DataFrame.{processTaxiData, readCSV, readParquet}
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.SharedSparkSession

class Test_DF extends SharedSparkSession {
  //import testImplicits._

  test("homework5 - DF") {
    val taxiZonesDF2 = readCSV("src/main/resources/data/taxi_zones.csv")
    val taxiDF2 = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")
    val actualDistribution = processTaxiData(taxiZonesDF2, taxiDF2)

    checkAnswer(
      actualDistribution,
        Row("Manhattan",304266)  ::
        Row("Queens",17712) ::
        Row("Unknown",6644) ::
        Row("Brooklyn",3037) ::
        Row("Bronx",211) ::
        Row("EWR",19) ::
        Row("Staten Island",4) :: Nil
    )
  }
}

