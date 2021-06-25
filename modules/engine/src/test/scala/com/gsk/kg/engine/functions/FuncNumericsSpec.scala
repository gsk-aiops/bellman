package com.gsk.kg.engine.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import com.gsk.kg.engine.compiler.SparkSpec

import org.scalatest.Assertion
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

  val inColName            = "in"
  val ceilExpectedColName  = "ceilExpected"
  val roundExpectedColName = "roundExpected"
  val absExpectedColName   = "absExpected"
  val nullValue            = null

  lazy val elems = List(
    (1.1, "2", "1.0", "1.1"),
    (1.4, "2", "1.0", "1.4"),
    (-0.3, "0", "0.0", "0.3"),
    (1.8, "2", "2.0", "1.8"),
    (10.5, "11", "11.0", "10.5"),
    (-10.5, "-10", "-11.0", "10.5")
  )
  lazy val df =
    elems.toDF(
      inColName,
      ceilExpectedColName,
      roundExpectedColName,
      absExpectedColName
    )

  lazy val typedElems = List[(String, String, String, String)](
    ("\"2\"^^xsd:int", "\"2\"^^xsd:int", "\"2\"^^xsd:int", "\"2\"^^xsd:int"),
    ("\"2.3\"^^xsd:int", nullValue, nullValue, nullValue),
    (
      "\"1\"^^xsd:integer",
      "\"1\"^^xsd:integer",
      "\"1\"^^xsd:integer",
      "\"1\"^^xsd:integer"
    ),
    (
      "\"-0.3\"^^xsd:decimal",
      "\"0\"^^xsd:decimal",
      "\"0.0\"^^xsd:decimal",
      "\"0.3\"^^xsd:decimal"
    ),
    (
      "\"10.5\"^^xsd:float",
      "\"11\"^^xsd:float",
      "\"11.0\"^^xsd:float",
      "\"10.5\"^^xsd:float"
    ),
    (
      "\"-10.5\"^^xsd:double",
      "\"-10\"^^xsd:double",
      "\"-11.0\"^^xsd:double",
      "\"10.5\"^^xsd:double"
    ),
    ("\"-10.5\"^^xsd:string", nullValue, nullValue, nullValue),
    ("2.8", "3", "3.0", "2.8"),
    ("2", "2", "2.0", "2")
  )
  lazy val typedDf =
    typedElems.toDF(
      inColName,
      ceilExpectedColName,
      roundExpectedColName,
      absExpectedColName
    )

  "FuncNumerics" when {

    "ceil function" should {

      "ceil function returns the smallest integer not smaller than" in {
        eval(df, FuncNumerics.ceil, ceilExpectedColName)
      }

      "multiple numeric types" in {
        eval(typedDf, FuncNumerics.ceil, ceilExpectedColName)
      }
    }

    "round function" should {

      "round function returns the smallest integer not smaller than" in {
        eval(df, FuncNumerics.round, roundExpectedColName)
      }

      "multiple numeric types" in {
        eval(typedDf, FuncNumerics.round, roundExpectedColName)
      }
    }

    "abs function" should {

      "abs function" in {
        eval(df, FuncNumerics.abs, absExpectedColName)
      }

      "abs with multiple numeric types" in {
        eval(typedDf, FuncNumerics.abs, absExpectedColName)
      }
    }
  }

  private def eval(
      df: DataFrame,
      f: Column => Column,
      expectedColName: String
  ): Assertion = {
    val dfR = df.select(f(col(inColName)))
    df.show()
    dfR.show()
    val expected = df.select(expectedColName)
    dfR.collect().toList shouldEqual expected.collect().toList
  }
}
