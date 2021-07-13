package com.gsk.kg.engine.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.functions.substring_index
import org.apache.spark.sql.functions.to_timestamp

import com.gsk.kg.engine.compiler.SparkSpec

import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class FuncDatesSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with ScalaCheckDrivenPropertyChecks {

  import sqlContext.implicits._

  override implicit def reuseContextIfPossible: Boolean = true
  override implicit def enableHiveSupport: Boolean      = false

  "FuncDates" when {

    implicit lazy val df: DataFrame =
      List(
        "\"2011-01-10T14:45:13.815-05:09\"^^xsd:dateTime",
        "\"2020-12-09T01:50:24Z\"^^xsd:dateTime"
      ).toDF()

    "now function" should {

      val nowColName = "now"
      val startPos   = 2

      "now function returns current date" in {
        val df            = List(1, 2, 3).toDF()
        val dfCurrentTime = df.select(FuncDates.now.as(nowColName))

        dfCurrentTime
          .select(
            to_timestamp(
              substring(
                substring_index(col(nowColName), "\"^^xsd:dateTime", 1),
                startPos,
                Int.MaxValue
              )
            ).isNotNull
          )
          .collect()
          .toSet shouldEqual Set(Row(true))
      }
    }

    "year function" should {

      val expected = Array(
        Row(2011),
        Row(2020)
      )

      "year function returns year of datetime" in {
        eval(FuncDates.year, expected)
      }
    }

    "month function" should {

      val expected = Array(
        Row(1),
        Row(12)
      )

      "month function returns month of datetime" in {
        eval(FuncDates.month, expected)
      }
    }

    "day function" should {
      val expected = Array(
        Row(10),
        Row(9)
      )

      "day function returns day of datetime" in {
        eval(FuncDates.day, expected)
      }
    }

    "hour function" should {

      val expected = Array(
        Row(14),
        Row(1)
      )

      "hour function returns hour of datetime" in {
        eval(FuncDates.hours, expected)
      }
    }

    "minutes function" should {
      val expected = Array(
        Row(45),
        Row(50)
      )

      "minutes function returns min of datetime" in {
        eval(FuncDates.minutes, expected)
      }
    }

    "seconds function" should {

      val expected = Array(
        Row(13.815),
        Row(24.0)
      )

      "seconds function returns seconds of datetime" in {
        eval(FuncDates.seconds, expected)
      }
    }

    "tz function" should {

      val expected = Array(
        Row("-05:09"),
        Row("Z")
      )

      "tz function returns tz of datetime" in {
        eval(FuncDates.tz, expected)
      }
    }
  }

  private def eval(f: Column => Column, expected: Array[Row])(implicit
      df: DataFrame
  ): Assertion = {
    val dfR =
      df.select(f(col(df.columns.head)).as("r"))

    dfR
      .select(col("r"))
      .collect() shouldEqual expected
  }
}
