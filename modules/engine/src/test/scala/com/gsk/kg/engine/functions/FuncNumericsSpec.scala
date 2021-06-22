package com.gsk.kg.engine.functions

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col

import com.gsk.kg.engine.compiler.SparkSpec

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class FuncNumericsSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with ScalaCheckDrivenPropertyChecks {

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

    "string representing numeric value" in {
      val elems    = List("2.8")
      val df       = elems.toDF()
      val dfR      = df.select(FuncNumerics.ceil(col(df.columns.head)))
      val expected = List(3).map(Row(_))

      dfR.collect().toList shouldEqual expected
    }

    "multiple numeric types" in {
      val elems = List(
        ("\"2\"^^xsd:int", 2),
        ("\"1\"^^xsd:integer", 1),
        ("\"-0.3\"^^xsd:decimal", 0),
        ("\"10.5\"^^xsd:float", 11),
        ("\"-10.5\"^^xsd:double", -10)
      )
      val df       = elems.toDF("in", "expected")
      val result   = df.select(FuncNumerics.ceil(df("in")))
      val expected = df.select(col("expected"))

      result.collect() shouldEqual expected.collect()
    }
  }
}
