package observatory

import java.nio.file.Paths
import java.time.LocalDate

import org.apache.spark.sql._
import org.apache.spark.sql.types._

/**
  * 1st milestone: data extraction
  */
object Extraction {
  val spark = SparkSession.builder().getOrCreate()

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    ???
  }

  /** @return The filesystem path of the given resource */
  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString

  def locateTemperaturesDF(year: Year, stationsFile: String, temperaturesFile: String): DataFrame = {
    val stationsRDD = spark.sparkContext.textFile(fsPath(stationsFile))
    val tempRDD = spark.sparkContext.textFile(fsPath(temperaturesFile))

    val stationsData =
      stationsRDD
        .map(_.split(",").to[List])
        .map(stationsRow)

    val tempData =
      tempRDD
        .map(_.split(",").to[List])
          .map(tempRow)
    val stationsDataFrame = spark.createDataFrame(stationsData, createStationsSchema)
    val tempDataFrame = spark.createDataFrame(tempData, createTempSchema)
  }

  def stationsRow(fields: List[String]) : Row =
    Row.fromTuple((fields(0).toInt, fields(1).toInt, fields(2).toDouble, fields(3).toDouble))

  def tempRow(fields: List[String]) : Row =
    Row.fromTuple(fields(0).toInt, fields(1).toInt, fields(2).toInt, fields(3).toInt, fields(4).toDouble)

  def createStationsSchema : StructType = {
    StructType(List(
      StructField("STN", IntegerType, nullable = true),
      StructField("WBAN", IntegerType, nullable = true),
      StructField("lat", DoubleType, nullable = true),
      StructField("lon", DoubleType, nullable = true)
    ))
  }

  def createTempSchema : StructType =
    StructType(List(
      StructField("STN", IntegerType, nullable = true),
      StructField("WBAN", IntegerType, nullable = true),
      StructField("month", IntegerType, nullable = true),
      StructField("day", IntegerType, nullable = true),
      StructField("temp", DoubleType, nullable = true)
    ))


  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    ???
  }

}
