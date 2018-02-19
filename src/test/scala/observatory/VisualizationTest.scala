package observatory


import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.Checkers


@RunWith(classOf[JUnitRunner])
class VisualizationTest extends FunSuite with Checkers {

  def roundScale(x: Double, scale: Int): Double = {
    BigDecimal(x).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  test("test Great Circle Distance") {
    val locationPairs = List(
      (Location(50.0, 0.0), Location(58.0, 0.0)),
      (Location(52.0, 4.0), Location(52.0, 21.0)),
      (Location(40.7, -74.0), Location(52.0, 4.0)),
      (Location(52.32, 12.34), Location(52.32, 12.34)),
      (Location(90,0), Location(-90,0)),
      (Location(90,0), Location(-90, 180)),
      (Location(51.12, -12.0), Location(-51.12, -12.0+180))
    )
    val antipodesDistance = roundScale(math.Pi*6371, 1)
    val distances = List(889.6, 1161.1, 5820.5, 0, antipodesDistance, antipodesDistance, antipodesDistance)

    val computedDistances = locationPairs.map(pair => {
      val dist = Visualization.greatCircleDistance(pair._1, pair._2)
      roundScale(dist, 1)
    })

    val pairedDistances = distances.zip(computedDistances)

    pairedDistances.foreach(pair => {
      println(s"Testing pair (${pair._1}, ${pair._2})")
      assert(pair._1 == pair._2)
    })

  }

  test("test spatial interpolation - single point") {
    val location = Location(50.0, 4.0)
    val temp = 32.1
    val points = List((location, temp))
    val res = Visualization.predictTemperature(points, location)
    assert(res == temp)
  }

  test("test spacial interpolation - one point equal") {
    val location = Location(-52.0, -10.0)
    val temp = 10.1
    val points = List((location, temp), (Location(32,32), -10.0))
    val res = Visualization.predictTemperature(points, location)
    assert(res == temp)
  }

  test("test spacial interpolation - half way through") {
    val loc1 = Location(52.0, 10.0)
    val loc2 = Location(52.0, 20.0)
    val temp1 = 10.0
    val temp2 = 20.0
    val testloc = Location(52.0, 15.0)
    val points = List((loc1, temp1), (loc2, temp2))
    val res = Visualization.predictTemperature(points, testloc)
    assert(roundScale(res,1) == 15.0)
  }

}
