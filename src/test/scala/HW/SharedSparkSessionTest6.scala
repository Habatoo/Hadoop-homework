package HW

import homework2.DataApiHomeWorkTaxi4.{calcPopularBorough, readDFFromCSV, readFromParquet}
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.SharedSparkSession

class SharedSparkSessionTest6 extends SharedSparkSession {

  // Сформировать ожидаемый результат и покрыть Spark тестом
  // (с библиотекой SharedSparkSession) витрину из домашнего
  // задания к занятию Spark Data API, построенную с помощью DF и DS.
  // Пример src/test/scala/lesson2/TestSharedSparkSession.scala

  test("Top Borough Test") {

    val taxiFactsDF = readFromParquet("src/main/resources/data/yellow_taxi_jan_25_2018")
    val taxiZoneDF = readDFFromCSV("src/main/resources/data/taxi_zones.csv")
    val resultDf = calcPopularBorough(taxiFactsDF, taxiZoneDF)

    checkAnswer(
      resultDf,
      Row("Manhattan", 295642) ::
        Row("Queens", 13394) ::
        Row("Brooklyn", 12587) ::
        Row("Unknown", 6285) ::
        Row("Bronx", 1562) ::
        Row("EWR", 491) ::
        Row("Staten Island", 62) :: Nil
    )
  }

}

