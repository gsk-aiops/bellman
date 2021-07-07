package com.gsk.kg.engine.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.functions.substring_index
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.functions.to_utc_timestamp
import com.gsk.kg.engine.compiler.SparkSpec
import com.gsk.kg.engine.functions.Literals.NumericLiteral
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
        "\"2012-04-10T10:45:13.815-01:00\"^^xsd:dateTime"
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

      val yearColName = "year"
      val expected    = Array(Row(2011))

      "year function returns year of datetime" in {
        eval(FuncDates.year, expected)
      }
    }

    "month function" should {

      val monthColName = "month"
      val expected     = Array(Row(1))

      "month function returns month of datetime" in {
        eval(FuncDates.month, expected)
      }
    }

    "hour function" should {

      val expected = Array(Row(11))

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

    df.withColumn(
      "timestamp",
      to_timestamp(NumericLiteral(df(df.columns.head)).value)
    ).withColumn(
      "utc",
      to_utc_timestamp(
        NumericLiteral(df(df.columns.head)).value,
        "Europe/Berlin"
      )
    ).show(false)
    dfR.show(false)

    dfR
      .select(col("r"))
      .collect() shouldEqual expected
  }
}
