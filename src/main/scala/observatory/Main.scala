package observatory

import org.apache.spark.sql.SparkSession

object Main extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Observatory")
      .config("spark.master", "local")
      .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")



}
