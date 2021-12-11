package homework2

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataApiHomeWorkTaxi4 extends App {

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/otus"
  val user = "docker"
  val password = "docker"

  implicit val spark = SparkSession.builder()
    .appName("HW_4")
    .config("spark.master", "local")
    .getOrCreate()

  // Логически разбить на методы, написанный в домашнем задании к занятию Spark Data API.

  // Метод для загрузки данных в DataSet из файла с фактическими данными поездок в Parquet
  // (src/main/resources/data/yellow_taxi_jan_25_2018).
  def readFromParquet(path: String)(implicit spark: SparkSession): DataFrame = spark.read.load(path)


  // Метод для загрузки данных во второй DataFrame из файла со справочными данными
  // поездок в csv (src/main/resources/data/taxi_zones.csv)
  def readDFFromCSV(pathToCSV: String)(implicit spark: SparkSession): DataFrame =
    spark.read
      .option("header", "true")
      .option("schema", "true")
      .csv(pathToCSV)


  // Метод для записи результата в Parquet
  def writeToParquet(resultDf: DataFrame, path: String)(implicit spark: SparkSession): Unit = {
    resultDf.repartition(1).write.mode("overwrite").parquet(path)
  }

  // Метод для постройки с помощью DSL таблицы,
  // которая покажет какие районы самые популярные для заказов.
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

  val taxiFactsDF = readFromParquet("src/main/resources/data/yellow_taxi_jan_25_2018")
  val taxiZoneDF = readDFFromCSV("src/main/resources/data/taxi_zones.csv")
  val resultDf = calcPopularBorough(taxiFactsDF, taxiZoneDF)
  resultDf.show()
  writeToParquet(resultDf, "out/resultDf_2")

  spark.stop()

}

