package homework2

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataApiHomeWorkTaxi extends App {

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/otus"
  val user = "docker"
  val password = "docker"

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()


  // Загрузить данные в первый DataFrame из файла с фактическими данными
  // поездок в Parquet (src/main/resources/data/yellow_taxi_jan_25_2018).
  val taxiFactsDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")

  // Загрузить данные во второй DataFrame из файла со справочными данными
  // поездок в csv (src/main/resources/data/taxi_zones.csv)
  val taxiZoneDF = spark.read
    .option("header", "true")
    .csv("src/main/resources/data/taxi_zones.csv")

  // С помощью DSL построить таблицу, которая покажет какие районы самые популярные для заказов.
  def calcPopularBorough(taxiFactsDF: DataFrame, taxiZoneDF: DataFrame) = {
    taxiFactsDF
      .select("DOLocationID", "trip_distance")
      .filter(col("trip_distance") > 0)
      .join(taxiZoneDF, col("DOLocationID") === col("LocationID"))
      .groupBy(col("Borough"))
      .agg(
        count(col("Borough")) as "count"
      )
      .sort(col("count").desc)

  }

  val topResultDf = calcPopularBorough(taxiFactsDF, taxiZoneDF)

  // Результат вывести на экран и записать в файл Паркет.
  topResultDf.show()
  topResultDf.repartition(1).write.mode("overwrite").parquet("out/resultDf")
  spark.stop()
}

