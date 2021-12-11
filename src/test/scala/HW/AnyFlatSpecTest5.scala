package HW

import homework2.DataApiHomeWorkTaxi4.readFromParquet
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, LocalTime}

class AnyFlatSpecTest5 extends AnyFlatSpec {

  implicit val spark = SparkSession.builder()
    .appName("HW_5")
    .config("spark.master", "local")
    .getOrCreate()

  // Сформировать ожидаемый результат и покрыть простым тестом
  // (с библиотекой AnyFlatSpec) витрину из домашнего задания
  // к занятию Spark Data API, построенную с помощью RDD.
  // Пример src/test/scala/lesson2/SimpleUnitTest.scala

  it should "Check load data" in {

    val taxiFactsDF = readFromParquet("src/main/resources/data/yellow_taxi_jan_25_2018")
    val topTimeRDD = taxiFactsDF.rdd
      .map(trip => (LocalDateTime.parse(
        trip(1).toString,
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S")).toLocalTime, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .take(1)

    assert(topTimeRDD(0)._1 == LocalTime.of(13, 51, 56))
    assert(topTimeRDD(0)._2 == 18)
  }

}

