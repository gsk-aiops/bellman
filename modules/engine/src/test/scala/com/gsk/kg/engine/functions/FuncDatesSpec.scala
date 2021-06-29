package com.gsk.kg.engine.functions

import com.gsk.kg.engine.compiler.SparkSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.types.TimestampType

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

      "now function returns current date" in {
        val now = FuncDates.now
        val df  = List(1, 2, 3).toDF()
        df.select(FuncDates.now.as("now")).show(false)

        val data = Seq(
          "07-01-2019 12 01 19 406",
          "06-24-2019 12 01 19 406",
          "11-16-2019 16 44 55 406",
          "11-16-2019 16 50 59 406"
        ).toDF("input_timestamp")
        data
          .withColumn(
            "datetype_timestamp",
            unix_timestamp(
              col("input_timestamp"),
              "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
            ).cast(TimestampType)
          )
          .show(false)

      }
    }
  }

}
