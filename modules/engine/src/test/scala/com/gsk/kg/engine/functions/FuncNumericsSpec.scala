package com.gsk.kg.engine.functions

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col

import com.gsk.kg.engine.compiler.SparkSpec

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class FuncNumericsSpec extends AnyWordSpec with Matchers with SparkSpec {

  import sqlContext.implicits._

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  "FuncNumerics.sample" should {

    "ceil function returns the smallest integer not smaller than" in {
      val elems    = List(1, 1.4, -0.3, 1.8, 10.5, -10.5)
      val df       = elems.toDF()
      val dfR      = df.select(FuncNumerics.ceil(col(df.columns.head)))
      val expected = List(1, 2, 0, 2, 11, -10).map(Row(_))

      dfR.collect().toList shouldEqual expected
    }
  }
}
