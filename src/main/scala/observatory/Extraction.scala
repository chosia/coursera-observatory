package observatory

import java.time.LocalDate

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

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
    val df = locateTemperaturesDF(year, stationsFile, temperaturesFile)
    df.collect.toList.map {
      row => (LocalDate.of(row(0).asInstanceOf[Int], row(1).asInstanceOf[Int], row(2).asInstanceOf[Int]),
        new Location(row(3).asInstanceOf[Double], row(4).asInstanceOf[Double]), row(5).asInstanceOf[Temperature])
    }
  }

  /** @return The filesystem path of the given resource */
  def fsPath(resource: String): String = {
    val rootPath = "/Users/chosia/scala-spark-workshop/observatory/src/main/resources/"
    rootPath + resource
  }

  def locateTemperaturesDF(year: Year, stationsFile: String, temperaturesFile: String): DataFrame = {
    val stationsRDD = spark.sparkContext.textFile(fsPath(stationsFile))
    val tempRDD = spark.sparkContext.textFile(fsPath(temperaturesFile))

    val stationsData =
      stationsRDD
        .map(_.split(",", -1).to[List])
        .map(stationsRow)

    val tempData =
      tempRDD
        .map(_.split(",", -1).to[List])
          .map(tempRow)
    val stationsDataFrame = spark.createDataFrame(stationsData, createStationsSchema)
    val tempDataFrame = spark.createDataFrame(tempData, createTempSchema).withColumn("year", lit(year))
    stationsDataFrame.show()
    tempDataFrame.filter("STN == 20580").show()

    val joinedDF = tempDataFrame
      .join(stationsDataFrame,
      tempDataFrame("STN") <=> stationsDataFrame("STN") &&
        tempDataFrame("WBAN") <=> stationsDataFrame("WBAN"))
        .select("year", "month", "day", "lat", "lon", "temp")
    joinedDF.show()
    joinedDF
  }

  def toIntOrNull(s: String) : Any = if (s.isEmpty) null else s.toInt

  def toDoubleOrNull(s: String) : Any = if (s.isEmpty) null else s.toDouble

  def stationsRow(fields: List[String]) : Row =
    Row.fromTuple((toIntOrNull(fields(0)), toIntOrNull(fields(1)), toDoubleOrNull(fields(2)), toDoubleOrNull(fields(3))))

  def tempRow(fields: List[String]) : Row =
    Row.fromTuple((toIntOrNull(fields(0)), toIntOrNull(fields(1)), toIntOrNull(fields(2)), toIntOrNull(fields(3)), toDoubleOrNull(fields(4))))

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
