package homework5

import homework5.TestingSparkApp_DataSet.testFunc
import org.apache.spark.sql.test.SharedSparkSession

class Test extends SharedSparkSession {
  test("homework5 - DS") {
    val test = testFunc("src/main/resources/data/yellow_taxi_jan_25_2018")
  }
}

