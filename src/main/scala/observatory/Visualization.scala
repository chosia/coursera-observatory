package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import observatory.Extraction.spark
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import scala.math._

/**
  * 2nd milestone: basic visualization
  */
object Visualization {

  def createLocTempSchema : StructType =
    StructType(
        StructField("lat", DoubleType, nullable = false) ::
        StructField("lon", DoubleType, nullable = false) ::
        StructField("temp", DoubleType, nullable = false) ::
        Nil
    )

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    val rdd = spark.sparkContext.parallelize(temperatures.toList)
      .map(r => Row(r._1.lat, r._1.lon, r._2))
    val schema = createLocTempSchema
    val df = spark.createDataFrame(rdd, schema)
    predictTemperatureDf(df, location)
  }

  def greatCircleDistance(location1: Location, location2: Location) : Double = {
    val lat1 = toRadians(location1.lat)
    val lat2 = toRadians(location2.lat)
    val lon1 = toRadians(location1.lon)
    val lon2 = toRadians(location2.lon)
    val centralAngle = acos(sin(lat1)*sin(lat2) + cos(lat1)*cos(lat2)*cos(lon1 - lon2))
    val earthRadius = 6371.0
    earthRadius * centralAngle
  }

  def predictTemperatureDf(temperatures: DataFrame, location: Location): Temperature = {
    return 0.0
  }


  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    ???
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    ???
  }

}

