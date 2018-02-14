package observatory

import org.apache.spark.sql.SparkSession
import observatory.Extraction._

object Main extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Observatory")
      .config("spark.master", "local")
      .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  locateTemperaturesDF(1975, "stations.csv", "1975.csv")

}
