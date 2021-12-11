package homework2

import org.apache.spark.sql.SparkSession

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object DataApiHomeWorkTaxi2 extends App {

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/otus"
  val user = "docker"
  val password = "docker"

  val spark = SparkSession.builder()
    .appName("HW_3")
    .config("spark.master", "local")
    .getOrCreate()


  // Загрузить данные в RDD из файла с фактическими данными поездок в Parquet
  // (src/main/resources/data/yellow_taxi_jan_25_2018).
  val taxiFactsDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")

  // С помощью lambda построить таблицу, которая покажет В какое время происходит больше всего вызовов.
  val topTimeRDD = taxiFactsDF.rdd
    .map(trip => (LocalDateTime.parse(
      trip(1).toString,
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S")).toLocalTime, 1))
    .reduceByKey(_ + _)
    .sortBy(_._2, ascending = false)

  // Результат вывести на экран и в txt файл c пробелами.
  topTimeRDD.foreach(println)
  topTimeRDD.take(20).foreach(println)
  topTimeRDD.repartition(1)
    .map(row => row._1.toString + " " + row._2.toString)
    .saveAsTextFile("out/topTimeRDDTxt")

  spark.stop()
}

