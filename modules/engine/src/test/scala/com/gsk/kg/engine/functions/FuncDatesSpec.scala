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
        "\"2012-04-10T10:45:13.815-01:00\"^^xsd:dateTime",
        "\"2020-12-09T01:50:24.815Z\"^^xsd:dateTime",
        "\"2019-02-25 03:50:24.815\"^^xsd:dateTime"
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
        Row(2012),
        Row(2020),
        Row(2019)
      )

      "year function returns year of datetime" in {
        eval(FuncDates.year, expected)
      }
    }

    "month function" should {

      val expected = Array(
        Row(4),
        Row(12),
        Row(2)
      )

      "month function returns month of datetime" in {
        eval(FuncDates.month, expected)
      }
    }

    "day function" should {
      val expected = Array(
        Row(10),
        Row(9),
        Row(25)
      )

      "day function returns day of datetime" in {
        eval(FuncDates.day, expected)
      }
    }

    "hour function" should {

      val expected = Array(
        Row(10),
        Row(1),
        Row(3)
      )

      "hour function returns hour of datetime" in {
        eval(FuncDates.hours, expected)
      }
    }
  }

  private def eval(f: Column => Column, expected: Array[Row])(implicit
      df: DataFrame
  ): Assertion = {
    val dfR =
      df.select(f(col(df.columns.head)).as("r"))

    dfR.show(false)

    dfR
      .select(col("r"))
      .collect() shouldEqual expected
  }
}
