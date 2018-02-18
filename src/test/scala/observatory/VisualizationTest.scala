package observatory


import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.Checkers

@RunWith(classOf[JUnitRunner])
class VisualizationTest extends FunSuite with Checkers {

  test("test Great Circle Distance") {
    val locationPairs = List(
      (Location(50.0, 0.0), Location(58.0, 0.0)),
      (Location(52.0, 4.0), Location(52.0, 21.0)),
      (Location(40.7, -74.0), Location(52.0, 4.0))
    )
    val distances = List(889.6, 1161.1, 5820.5)

    val computedDistances = locationPairs.map(pair => {
      val dist = Visualization.greatCircleDistance(pair._1, pair._2)
      BigDecimal(dist).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
    })

    val pairedDistances = distances.zip(computedDistances)

    pairedDistances.foreach(pair => assert(pair._1 == pair._2))

  }

}
