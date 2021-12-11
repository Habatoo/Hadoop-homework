package homework2

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties

object DataApiHomeWorkTaxi3 extends App {

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/otus"
  val user = "docker"
  val password = "docker"

  val spark = SparkSession.builder()
    .appName("HW_3")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  case class TaxiZoneTripDF(
                             borough: String,
                             trip_distance: Double
                           )

  // Загрузить данные в DataSet из файла с фактическими данными поездок в Parquet
  // (src/main/resources/data/yellow_taxi_jan_25_2018).
  val taxiFactsDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
  val taxiZoneDF = spark.read
    .option("header", "true")
    .csv("src/main/resources/data/taxi_zones.csv")

  // С помощью DSL и lambda построить таблицу, которая покажет.
  // Как происходит распределение поездок по дистанции?
  val distanceDistribution = taxiFactsDF.select("DOLocationID", "trip_distance")
    .join(taxiZoneDF, taxiFactsDF("DOLocationID") === taxiZoneDF("LocationID"))
    .select(col("Borough"), col("trip_distance")).as[TaxiZoneTripDF]
    .filter(tz => tz.trip_distance > 0)
    .groupBy(taxiZoneDF("Borough") as "borough")
    .agg(
      count(taxiZoneDF("Borough")) as "count",
      round(mean(taxiFactsDF("trip_distance")), 2) as "mean_distance",
      round(stddev(taxiFactsDF("trip_distance")), 2) as "std_distance",
      round(min(taxiFactsDF("trip_distance")), 2) as "min_distance",
      round(max(taxiFactsDF("trip_distance")), 2) as "max_distance",
    )
    .sort(col("count").desc)

  // Результат вывести на экран и записать в бд Постгрес (докер в проекте).
  // Для записи в базу данных необходимо продумать и также приложить инит sql файл со структурой.
  val connectionProperties = new Properties()
  connectionProperties.put("user", user)
  connectionProperties.put("password", password)
  connectionProperties.put("driver", driver)

  distanceDistribution
    .write
    .mode(SaveMode.Overwrite)
    .jdbc(url, "postgresTable", connectionProperties)
  //(Пример: можно построить витрину со следующими колонками: общее количество поездок, среднее расстояние,
  // среднеквадратическое отклонение, минимальное и максимальное расстояние)

  distanceDistribution.show(10)

  spark.stop()
}

