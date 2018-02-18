package observatory

import java.time.LocalDate

import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class ExtractionTest extends FunSuite {
  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Observatory")
      .config("spark.master", "local")
      .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  test("test locateTemperatures") {
    val expectedResult = Seq(
      (LocalDate.of(2015, 8, 11), Location(37.35, -78.433), 27.3),
      (LocalDate.of(2015, 12, 6), Location(37.358, -78.438), 0.0),
      (LocalDate.of(2015, 1, 29), Location(37.358, -78.438), 2.0)
    )
    val result = Extraction.locateTemperatures(2015, "stations-test.csv", "temps-test.csv")
    println("Result:")
    println(result.mkString(",\n"))
    println("Expected result:")
    println(expectedResult.mkString(",\n"))
    assert(result.toSet == expectedResult.toSet)
  }

  test("test locationYearlyAverageRecords") {
    val expectedResult = Seq(
      (Location(37.35, -78.433), 27.3),
      (Location(37.358, -78.438), 1.0)
    )
    val tmp = Extraction.locateTemperatures(2015, "stations-test.csv", "temps-test.csv")
    val result = Extraction.locationYearlyAverageRecords(tmp)
    assert(result.toSet == expectedResult.toSet)
  }

  test("test from forum") {
    val expectedResult = mutable.ArraySeq(
      (LocalDate.of(2000,1,1),Location(1.0,-1.0),-12.222222222222223),
      (LocalDate.of(2000,1,2),Location(2.0,-2.0),-12.222222222222223),
      (LocalDate.of(2000,1,3),Location(3.0,-3.0),-12.222222222222223),
      (LocalDate.of(2000,1,4),Location(4.0,-4.0),-12.222222222222223),
      (LocalDate.of(2000,1,5),Location(5.0,-5.0),-12.222222222222223)
    )
    val result = Extraction.locateTemperatures(2000, "stations-test1.csv", "temps-test2.csv").toSet
    assert(result.size == 5)
    assert(result == expectedResult.toSet)
  }
  
}