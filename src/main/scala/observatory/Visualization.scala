package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import observatory.Extraction.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
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
    val rdd: RDD[(Location, Temperature)] = spark.sparkContext.parallelize(temperatures.toList)
    //  .map(r => Row(r._1.lat, r._1.lon, r._2))
    //val schema = createLocTempSchema
    //val df = spark.createDataFrame(rdd, schema)
    predictTemperatureRdd(rdd, location)
  }

  def greatCircleDistance(location1: Location, location2: Location) : Double = {
    val centralAngle =
      /* Test for the same location */
      if (location1 == location2)
        0
      /* Test for antipodes */
      else if (location1.lat == -location2.lat && (location1.lon == location2.lon + 180 || location1.lon == location2.lon - 180))
        Pi
      else {
        val lat1 = toRadians(location1.lat)
        val lat2 = toRadians(location2.lat)
        val lon1 = toRadians(location1.lon)
        val lon2 = toRadians(location2.lon)
        acos(sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(lon1 - lon2))
      }
    val earthRadius = 6371.0
    earthRadius * centralAngle
  }

  def predictTemperatureRdd(temperatures: RDD[(Location, Temperature)], location: Location): Temperature = {
    val p = 2.0
    val tempsWithDistance = temperatures.map(row => {
      val dist = greatCircleDistance(row._1, location)
      (row._2, dist)
    })
    /* Find the closest point */
    val (temp, dist) = tempsWithDistance.reduce((a,b) => if (a._2 < b._2) a else b)
    if (dist < 1.0) {
      /* If the smallest distance is less than 1km, return the temperature at this closest point */
      temp
    } else {
      /* Otherwise apply inverse distance weighting */
      val weightedTemps = tempsWithDistance.map(row => {
        val weight = 1.0 / pow(row._2, p)
        (row._1 * weight, weight)
      })
      val (a, b) = weightedTemps.reduce((a, b) => (a._1 + b._1, a._2 + b._2))
      a / b
    }
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

