package com.gsk.kg.engine.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import com.gsk.kg.engine.compiler.SparkSpec
import com.gsk.kg.engine.functions.Literals.NumericLiteral
import com.gsk.kg.engine.functions.Literals.isDoubleNumericLiteral
import org.apache.spark.sql.types.DoubleType
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
  val randExpectedColName  = "randExpected"
  val nullValue            = null

  lazy val elems = List(
    (1.1, "2", "1.0", true),
    (1.4, "2", "1.0", true),
    (-0.3, "0", "0.0", true),
    (1.8, "2", "2.0", true),
    (10.5, "11", "11.0", true),
    (-10.5, "-10", "-11.0", true)
  )
  lazy val df =
    elems.toDF(
      inColName,
      ceilExpectedColName,
      roundExpectedColName,
      randExpectedColName
    )

  lazy val typedElems = List[(String, String, String, Boolean)](
    ("\"2\"^^xsd:int", "\"2\"^^xsd:int", "\"2\"^^xsd:int", true),
    ("\"2.3\"^^xsd:int", nullValue, nullValue, true),
    ("\"1\"^^xsd:integer", "\"1\"^^xsd:integer", "\"1\"^^xsd:integer", true),
    (
      "\"-0.3\"^^xsd:decimal",
      "\"0\"^^xsd:decimal",
      "\"0.0\"^^xsd:decimal",
      true
    ),
    ("\"10.5\"^^xsd:float", "\"11\"^^xsd:float", "\"11.0\"^^xsd:float", true),
    (
      "\"-10.5\"^^xsd:double",
      "\"-10\"^^xsd:double",
      "\"-11.0\"^^xsd:double",
      true
    ),
    ("\"-10.5\"^^xsd:string", nullValue, nullValue, true),
    ("2.8", "3", "3.0", true),
    ("2", "2", "2.0", true)
  )
  lazy val typedDf =
    typedElems.toDF(
      inColName,
      ceilExpectedColName,
      roundExpectedColName,
      randExpectedColName
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

    "rand function" should {

      "rand function" in {
        eval(df, FuncNumerics.rand, randExpectedColName)
      }

      "rand with multiple numeric types" in {
        eval(typedDf, FuncNumerics.rand, randExpectedColName)
      }
    }
  }

  private def eval(
      df: DataFrame,
      f: Column => Column,
      expectedColName: String
  ): Assertion = {
    val dfR      = df.select(f(col(inColName)))
    val expected = df.select(expectedColName)
    dfR.collect().toList shouldEqual expected.collect().toList
  }

  private def eval(
      df: DataFrame,
      f: Column,
      expectedColName: String
  ): Assertion = {
    val dfR = df
      .select(f.as("r"))
      .select(
        isDoubleNumericLiteral(col("r")) &&
          NumericLiteral(col("r")).value.cast(DoubleType).isNotNull
      )
    val expected = Set(Row(true))
    dfR.collect().toSet shouldEqual expected
  }
}
