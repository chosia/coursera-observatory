package observatory

import org.apache.spark.sql._
import observatory.Extraction._

object Main extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Observatory")
      .config("spark.master", "local")
      .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  /*
  var df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], Extraction.createLocatedTempSchema)
  for (year <- 1975 to 2015) {
    val newDf = locateTemperaturesDF(year, "stations.csv", year.toString + ".csv")
    df = df.union(newDf)
  }
  val avgDf = locationYearlyAverageRecordsDf(df)
  avgDf.explain()
  val cnt = avgDf.count()
  println(s"AvgDf has $cnt rows")
  avgDf.show()
  */
  val temps = locateTemperatures(1975, "stations.csv", "1975.csv")
  val avgTemps = locationYearlyAverageRecords(temps)
  println(avgTemps.mkString(",\n"))

}
