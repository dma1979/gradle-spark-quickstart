/*
 * This Scala Testsuite was generated by the Gradle 'init' task.
 */
package gradle.spark.quickstart

import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AppSuite extends AnyFunSuite {
  test("App has a greeting") {
    assert(App.some().collectAsList().get(0) == 1)
  }
}