package com.gsk.kg.engine.functions

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
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
      val len        = 29

      "now function returns current date" in {
        val df             = List(1, 2, 3).toDF()
        val dfCurrentTime  = df.select(FuncDates.now.as(nowColName))
        val dfCurrentTime2 = df.select(FuncDates.now1.as(nowColName))
        val dfCurrentTime3 = df.select(FuncDates.now2.as(nowColName))

        dfCurrentTime.show(false)
        dfCurrentTime2.show(false)
        dfCurrentTime3.show(false)

        dfCurrentTime
          .select(to_timestamp(col(nowColName).substr(startPos, len)).isNotNull)
          .collect()
          .toSet shouldEqual Set(Row(true))
      }
    }
  }

}
