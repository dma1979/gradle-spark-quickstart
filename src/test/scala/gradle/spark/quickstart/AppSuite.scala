/*
 * This Scala Testsuite was generated by the Gradle 'init' task.
 */
package gradle.spark.quickstart

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.util

@RunWith(classOf[JUnitRunner])
class AppSuite extends AnyFunSuite {
  val spark = SparkSession.builder().master("local").getOrCreate()
  test("Movies have correct genres") {
    import spark.implicits._
    val movieWithGenres = App.pipeline(
      Seq(
        Movie(1L, "MyMovie (1994)", "Drama|Comedy|Thriller")
      ).toDS()
    )(spark)
      .collectAsList()
    assert(movieWithGenres.containsAll(util.Arrays.asList(
      MovieWithSingleGenre(1L, "MyMovie", "Drama", 1994),
      MovieWithSingleGenre(1L, "MyMovie", "Comedy", 1994),
      MovieWithSingleGenre(1L, "MyMovie", "Thriller", 1994)
    )))
  }
}
