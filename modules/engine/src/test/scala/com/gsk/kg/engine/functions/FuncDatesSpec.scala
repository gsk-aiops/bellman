package com.gsk.kg.engine.functions

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.functions.substring_index
import org.apache.spark.sql.functions.to_timestamp

import com.gsk.kg.engine.compiler.SparkSpec

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
        val df =
          List(
            "\"2011-01-10T14:45:13.815-05:00\"^^xsd:dateTime"
          ).toDF()
        val yearDf =
          df.select(FuncDates.year(col(df.columns.head)).as(yearColName))

        yearDf
          .select(col(yearColName))
          .collect() shouldEqual expected
      }
    }
  }

}
