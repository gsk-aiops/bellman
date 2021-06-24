package com.gsk.kg.engine.functions

import org.apache.spark.sql.functions.col

import com.gsk.kg.engine.compiler.SparkSpec
import com.gsk.kg.engine.scalacheck.CommonGenerators

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class FuncArithmeticSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with ScalaCheckDrivenPropertyChecks
    with CommonGenerators {

  import sqlContext.implicits._

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  "Funcs on Arithmetics" when {

    "FuncArithmetics.add" should {

      "operate on numbers correctly" when {

        "first argument is plain literal and second is numeric literal" in {

          val df = List(
            ("1", "1", "2.0"),
            ("1", "\"1\"^^xsd:int", "\"2\"^^xsd:int"),
            ("1", "\"1\"^^xsd:integer", "\"2\"^^xsd:integer"),
            ("1", "\"1\"^^xsd:decimal", "\"2\"^^xsd:decimal"),
            ("1.23", "\"1.23\"^^xsd:float", "\"2.46\"^^xsd:float"),
            ("1.23", "\"1.23\"^^xsd:double", "\"2.46\"^^xsd:double"),
            ("1.23", "\"1.23\"^^xsd:numeric", "\"2.46\"^^xsd:numeric")
          ).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncArithmetics.add(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INT typed integer and second is numeric literal" in {

          val df = List(
            ("\"1\"^^xsd:int", "1", "\"2\"^^xsd:int"),
            ("\"1\"^^xsd:int", "1.5", "\"2\"^^xsd:int"),
            ("\"1\"^^xsd:int", "\"1\"^^xsd:int", "\"2\"^^xsd:int"),
            ("\"1\"^^xsd:int", "\"1\"^^xsd:integer", "\"2\"^^xsd:integer"),
            ("\"1\"^^xsd:int", "\"1\"^^xsd:decimal", "\"2\"^^xsd:decimal"),
            ("\"1\"^^xsd:int", "\"1.5\"^^xsd:float", "\"2.5\"^^xsd:float"),
            ("\"1\"^^xsd:int", "\"1.5\"^^xsd:double", "\"2.5\"^^xsd:double"),
            ("\"1\"^^xsd:int", "\"1.5\"^^xsd:numeric", "\"2.5\"^^xsd:numeric")
          ).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncArithmetics.add(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INTEGER typed integer and second is numeric literal" in {

          val df = List(
            ("\"1\"^^xsd:integer", "1", "\"2\"^^xsd:integer"),
            ("\"1\"^^xsd:integer", "1.0", "\"2\"^^xsd:integer"),
            ("\"1\"^^xsd:integer", "\"1\"^^xsd:int", "\"2\"^^xsd:int"),
            ("\"1\"^^xsd:integer", "\"1\"^^xsd:integer", "\"2\"^^xsd:integer"),
            ("\"1\"^^xsd:integer", "\"1\"^^xsd:decimal", "\"2\"^^xsd:decimal"),
            ("\"1\"^^xsd:integer", "\"1.0\"^^xsd:float", "\"2.0\"^^xsd:float"),
            (
              "\"1\"^^xsd:integer",
              "\"1.0\"^^xsd:double",
              "\"2.0\"^^xsd:double"
            ),
            (
              "\"1\"^^xsd:integer",
              "\"1.0\"^^xsd:numeric",
              "\"2.0\"^^xsd:numeric"
            )
          ).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncArithmetics.add(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DECIMAL typed integer and second is numeric literal" in {

          val df = List(
            ("\"1\"^^xsd:decimal", "1", "\"2\"^^xsd:decimal"),
            ("\"1\"^^xsd:decimal", "1.0", "\"2\"^^xsd:decimal"),
            ("\"1\"^^xsd:decimal", "\"1\"^^xsd:int", "\"2\"^^xsd:decimal"),
            ("\"1\"^^xsd:decimal", "\"1\"^^xsd:integer", "\"2\"^^xsd:decimal"),
            ("\"1\"^^xsd:decimal", "\"1\"^^xsd:decimal", "\"2\"^^xsd:decimal"),
            ("\"1\"^^xsd:decimal", "\"1.0\"^^xsd:float", "\"2.0\"^^xsd:float"),
            (
              "\"1\"^^xsd:decimal",
              "\"1.0\"^^xsd:double",
              "\"2.0\"^^xsd:double"
            ),
            (
              "\"1\"^^xsd:decimal",
              "\"1.0\"^^xsd:numeric",
              "\"2.0\"^^xsd:numeric"
            )
          ).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncArithmetics.add(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is FLOAT typed integer and second is numeric literal" in {

          val df = List(
            ("\"1.23\"^^xsd:float", "1.23", "\"2.46\"^^xsd:float"),
            ("\"1.0\"^^xsd:float", "\"1\"^^xsd:int", "\"2.0\"^^xsd:float"),
            ("\"1.0\"^^xsd:float", "\"1\"^^xsd:integer", "\"2.0\"^^xsd:float"),
            ("\"1.0\"^^xsd:float", "\"1\"^^xsd:decimal", "\"2.0\"^^xsd:float"),
            (
              "\"1.23\"^^xsd:float",
              "\"1.23\"^^xsd:float",
              "\"2.46\"^^xsd:float"
            ),
            (
              "\"1.23\"^^xsd:float",
              "\"1.23\"^^xsd:double",
              "\"2.46\"^^xsd:double"
            ),
            (
              "\"1.23\"^^xsd:float",
              "\"1.23\"^^xsd:numeric",
              "\"2.46\"^^xsd:numeric"
            )
          ).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncArithmetics.add(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DOUBLE typed integer and second is numeric literal" in {

          val df = List(
            ("\"1.23\"^^xsd:double", "1.23", "\"2.46\"^^xsd:double"),
            ("\"1.0\"^^xsd:double", "\"1\"^^xsd:int", "\"2.0\"^^xsd:double"),
            (
              "\"1.0\"^^xsd:double",
              "\"1\"^^xsd:integer",
              "\"2.0\"^^xsd:double"
            ),
            (
              "\"1.23\"^^xsd:double",
              "\"1.23\"^^xsd:decimal",
              "\"2.46\"^^xsd:double"
            ),
            (
              "\"1.23\"^^xsd:double",
              "\"1.23\"^^xsd:float",
              "\"2.46\"^^xsd:double"
            ),
            (
              "\"1.23\"^^xsd:double",
              "\"1.23\"^^xsd:double",
              "\"2.46\"^^xsd:double"
            ),
            (
              "\"1.23\"^^xsd:double",
              "\"1.23\"^^xsd:numeric",
              "\"2.46\"^^xsd:numeric"
            )
          ).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncArithmetics.add(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is NUMERIC typed integer and second is numeric literal" in {

          val df = List(
            ("\"1.23\"^^xsd:numeric", "1.23", "\"2.46\"^^xsd:numeric"),
            ("\"1.0\"^^xsd:numeric", "\"1\"^^xsd:int", "\"2.0\"^^xsd:numeric"),
            (
              "\"1.0\"^^xsd:numeric",
              "\"1\"^^xsd:integer",
              "\"2.0\"^^xsd:numeric"
            ),
            (
              "\"1.23\"^^xsd:numeric",
              "\"1.23\"^^xsd:decimal",
              "\"2.46\"^^xsd:numeric"
            ),
            (
              "\"1.23\"^^xsd:numeric",
              "\"1.23\"^^xsd:float",
              "\"2.46\"^^xsd:numeric"
            ),
            (
              "\"1.23\"^^xsd:numeric",
              "\"1.23\"^^xsd:double",
              "\"2.46\"^^xsd:double"
            ),
            (
              "\"1.23\"^^xsd:numeric",
              "\"1.23\"^^xsd:numeric",
              "\"2.46\"^^xsd:numeric"
            )
          ).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncArithmetics.add(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }
      }
    }

    "FuncArithmetics.subtract" should {

      "operate on numbers correctly" when {

        "first argument is plain literal and second is numeric literal" in {

          val df = List(
            ("1", "1", "0.0"),
            ("1", "\"1\"^^xsd:int", "\"0\"^^xsd:int"),
            ("1", "\"-1\"^^xsd:integer", "\"2\"^^xsd:integer"),
            ("-1", "\"1\"^^xsd:decimal", "\"-2\"^^xsd:decimal"),
            ("1.23", "\"1.23\"^^xsd:float", "\"0.0\"^^xsd:float"),
            ("1.23", "\"-1.23\"^^xsd:double", "\"2.46\"^^xsd:double"),
            ("-1.23", "\"1.23\"^^xsd:numeric", "\"-2.46\"^^xsd:numeric")
          ).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncArithmetics.subtract(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INT typed integer and second is numeric literal" in {

          val df = List(
            ("\"1\"^^xsd:int", "1", "\"0\"^^xsd:int"),
            ("\"1\"^^xsd:int", "1.5", "\"0\"^^xsd:int"),
            ("\"1\"^^xsd:int", "\"1\"^^xsd:int", "\"0\"^^xsd:int"),
            ("\"1\"^^xsd:int", "\"-1\"^^xsd:integer", "\"2\"^^xsd:integer"),
            ("\"-1\"^^xsd:int", "\"1\"^^xsd:decimal", "\"-2\"^^xsd:decimal"),
            ("\"1\"^^xsd:int", "\"1.5\"^^xsd:float", "\"-0.5\"^^xsd:float"),
            ("\"1\"^^xsd:int", "\"1.5\"^^xsd:double", "\"-0.5\"^^xsd:double"),
            ("\"1\"^^xsd:int", "\"1.5\"^^xsd:numeric", "\"-0.5\"^^xsd:numeric")
          ).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncArithmetics.subtract(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INTEGER typed integer and second is numeric literal" in {

          val df = List(
            ("\"1\"^^xsd:integer", "1", "\"0\"^^xsd:integer"),
            ("\"1\"^^xsd:integer", "1.0", "\"0\"^^xsd:integer"),
            ("\"1\"^^xsd:integer", "\"1\"^^xsd:int", "\"0\"^^xsd:int"),
            (
              "\"-1\"^^xsd:integer",
              "\"1\"^^xsd:integer",
              "\"-2\"^^xsd:integer"
            ),
            ("\"1\"^^xsd:integer", "\"1\"^^xsd:decimal", "\"0\"^^xsd:decimal"),
            ("\"1\"^^xsd:integer", "\"1.0\"^^xsd:float", "\"0.0\"^^xsd:float"),
            (
              "\"1\"^^xsd:integer",
              "\"1.0\"^^xsd:double",
              "\"0.0\"^^xsd:double"
            ),
            (
              "\"1\"^^xsd:integer",
              "\"1.0\"^^xsd:numeric",
              "\"0.0\"^^xsd:numeric"
            )
          ).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncArithmetics.subtract(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DECIMAL typed integer and second is numeric literal" in {

          val df = List(
            ("\"1\"^^xsd:decimal", "1", "\"0\"^^xsd:decimal"),
            ("\"1\"^^xsd:decimal", "1.0", "\"0\"^^xsd:decimal"),
            ("\"1\"^^xsd:decimal", "\"1\"^^xsd:int", "\"0\"^^xsd:decimal"),
            ("\"1\"^^xsd:decimal", "\"1\"^^xsd:integer", "\"0\"^^xsd:decimal"),
            ("\"1\"^^xsd:decimal", "\"1\"^^xsd:decimal", "\"0\"^^xsd:decimal"),
            ("\"1\"^^xsd:decimal", "\"1.0\"^^xsd:float", "\"0.0\"^^xsd:float"),
            (
              "\"1\"^^xsd:decimal",
              "\"-1.5\"^^xsd:double",
              "\"2.5\"^^xsd:double"
            ),
            (
              "\"-1\"^^xsd:decimal",
              "\"1.5\"^^xsd:numeric",
              "\"-2.5\"^^xsd:numeric"
            )
          ).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncArithmetics.subtract(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is FLOAT typed integer and second is numeric literal" in {

          val df = List(
            ("\"1.23\"^^xsd:float", "1.23", "\"0.0\"^^xsd:float"),
            ("\"1.0\"^^xsd:float", "\"1\"^^xsd:int", "\"0.0\"^^xsd:float"),
            ("\"1.5\"^^xsd:float", "\"1\"^^xsd:integer", "\"0.5\"^^xsd:float"),
            (
              "\"-1.0\"^^xsd:float",
              "\"1\"^^xsd:decimal",
              "\"-2.0\"^^xsd:float"
            ),
            (
              "\"1.23\"^^xsd:float",
              "\"1.23\"^^xsd:float",
              "\"0.0\"^^xsd:float"
            ),
            (
              "\"-1.23\"^^xsd:float",
              "\"1.23\"^^xsd:double",
              "\"-2.46\"^^xsd:double"
            ),
            (
              "\"1.23\"^^xsd:float",
              "\"-1.23\"^^xsd:numeric",
              "\"2.46\"^^xsd:numeric"
            )
          ).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncArithmetics.subtract(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DOUBLE typed integer and second is numeric literal" in {

          val df = List(
            ("\"1.23\"^^xsd:double", "1.23", "\"0.0\"^^xsd:double"),
            ("\"1.0\"^^xsd:double", "\"1\"^^xsd:int", "\"0.0\"^^xsd:double"),
            (
              "\"1.0\"^^xsd:double",
              "\"1\"^^xsd:integer",
              "\"0.0\"^^xsd:double"
            ),
            (
              "\"-1.5\"^^xsd:double",
              "\"1\"^^xsd:decimal",
              "\"-2.5\"^^xsd:double"
            ),
            (
              "\"1.23\"^^xsd:double",
              "\"-1.23\"^^xsd:float",
              "\"2.46\"^^xsd:double"
            ),
            (
              "\"1.23\"^^xsd:double",
              "\"1.23\"^^xsd:double",
              "\"0.0\"^^xsd:double"
            ),
            (
              "\"2.0\"^^xsd:double",
              "\"1.5\"^^xsd:numeric",
              "\"0.5\"^^xsd:numeric"
            )
          ).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncArithmetics.subtract(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is NUMERIC typed integer and second is numeric literal" in {

          val df = List(
            ("\"1.23\"^^xsd:numeric", "1.23", "\"0.0\"^^xsd:numeric"),
            ("\"1.0\"^^xsd:numeric", "\"1\"^^xsd:int", "\"0.0\"^^xsd:numeric"),
            (
              "\"1.5\"^^xsd:numeric",
              "\"1\"^^xsd:integer",
              "\"0.5\"^^xsd:numeric"
            ),
            (
              "\"-1.23\"^^xsd:numeric",
              "\"1.23\"^^xsd:decimal",
              "\"-2.46\"^^xsd:numeric"
            ),
            (
              "\"1.23\"^^xsd:numeric",
              "\"-1.23\"^^xsd:float",
              "\"2.46\"^^xsd:numeric"
            ),
            (
              "\"1.5\"^^xsd:numeric",
              "\"0.5\"^^xsd:double",
              "\"1.0\"^^xsd:double"
            ),
            (
              "\"1.23\"^^xsd:numeric",
              "\"1.23\"^^xsd:numeric",
              "\"0.0\"^^xsd:numeric"
            )
          ).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncArithmetics.subtract(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }
      }
    }

    "FuncArithmetics.multiply" should {

      "operate on numbers correctly" when {

        "first argument is plain literal and second is numeric literal" in {

          val df = List(
            ("1", "1", "1.0"),
            ("1", "\"2\"^^xsd:int", "\"2\"^^xsd:int"),
            ("1", "\"0\"^^xsd:integer", "\"0\"^^xsd:integer"),
            ("0", "\"1\"^^xsd:decimal", "\"0\"^^xsd:decimal"),
            ("1.5", "\"2.0\"^^xsd:float", "\"3.0\"^^xsd:float"),
            ("1.5", "\"-2.0\"^^xsd:double", "\"-3.0\"^^xsd:double"),
            ("-1.5", "\"2.0\"^^xsd:numeric", "\"-3.0\"^^xsd:numeric")
          ).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncArithmetics.multiply(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INT typed integer and second is numeric literal" in {

          val df = List(
            ("\"1\"^^xsd:int", "1", "\"1\"^^xsd:int"),
            ("\"1\"^^xsd:int", "1.5", "\"1\"^^xsd:int"),
            ("\"1\"^^xsd:int", "\"2\"^^xsd:int", "\"2\"^^xsd:int"),
            ("\"1\"^^xsd:int", "\"-2\"^^xsd:integer", "\"-2\"^^xsd:integer"),
            ("\"1\"^^xsd:int", "\"1\"^^xsd:decimal", "\"1\"^^xsd:decimal"),
            ("\"1\"^^xsd:int", "\"-1.5\"^^xsd:float", "\"-1.5\"^^xsd:float"),
            ("\"1\"^^xsd:int", "\"1.5\"^^xsd:double", "\"1.5\"^^xsd:double"),
            ("\"1\"^^xsd:int", "\"1.5\"^^xsd:numeric", "\"1.5\"^^xsd:numeric")
          ).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncArithmetics.multiply(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INTEGER typed integer and second is numeric literal" in {

          val df = List(
            ("\"1\"^^xsd:integer", "1", "\"1\"^^xsd:integer"),
            ("\"1\"^^xsd:integer", "-1.0", "\"-1\"^^xsd:integer"),
            ("\"-1\"^^xsd:integer", "\"2\"^^xsd:int", "\"-2\"^^xsd:int"),
            ("\"1\"^^xsd:integer", "\"1\"^^xsd:integer", "\"1\"^^xsd:integer"),
            (
              "\"1\"^^xsd:integer",
              "\"-1\"^^xsd:decimal",
              "\"-1\"^^xsd:decimal"
            ),
            (
              "\"-1\"^^xsd:integer",
              "\"1.0\"^^xsd:float",
              "\"-1.0\"^^xsd:float"
            ),
            (
              "\"1\"^^xsd:integer",
              "\"2.5\"^^xsd:double",
              "\"2.5\"^^xsd:double"
            ),
            (
              "\"-1\"^^xsd:integer",
              "\"1.5\"^^xsd:numeric",
              "\"-1.5\"^^xsd:numeric"
            )
          ).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncArithmetics.multiply(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DECIMAL typed integer and second is numeric literal" in {

          val df = List(
            ("\"1\"^^xsd:decimal", "1", "\"1\"^^xsd:decimal"),
            ("\"1\"^^xsd:decimal", "-1.0", "\"-1\"^^xsd:decimal"),
            ("\"1\"^^xsd:decimal", "\"1\"^^xsd:int", "\"1\"^^xsd:decimal"),
            (
              "\"-1\"^^xsd:decimal",
              "\"1\"^^xsd:integer",
              "\"-1\"^^xsd:decimal"
            ),
            ("\"2\"^^xsd:decimal", "\"1\"^^xsd:decimal", "\"2\"^^xsd:decimal"),
            (
              "\"1\"^^xsd:decimal",
              "\"-1.5\"^^xsd:float",
              "\"-1.5\"^^xsd:float"
            ),
            (
              "\"1\"^^xsd:decimal",
              "\"1.5\"^^xsd:double",
              "\"1.5\"^^xsd:double"
            ),
            (
              "\"1\"^^xsd:decimal",
              "\"-1.0\"^^xsd:numeric",
              "\"-1.0\"^^xsd:numeric"
            )
          ).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncArithmetics.multiply(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is FLOAT typed integer and second is numeric literal" in {

          val df = List(
            ("\"1.5\"^^xsd:float", "1.5", "\"2.25\"^^xsd:float"),
            ("\"1.5\"^^xsd:float", "\"1\"^^xsd:int", "\"1.5\"^^xsd:float"),
            (
              "\"1.5\"^^xsd:float",
              "\"-1\"^^xsd:integer",
              "\"-1.5\"^^xsd:float"
            ),
            (
              "\"-1.5\"^^xsd:float",
              "\"1\"^^xsd:decimal",
              "\"-1.5\"^^xsd:float"
            ),
            (
              "\"1.5\"^^xsd:float",
              "\"1.5\"^^xsd:float",
              "\"2.25\"^^xsd:float"
            ),
            (
              "\"1.5\"^^xsd:float",
              "\"-2\"^^xsd:double",
              "\"-3.0\"^^xsd:double"
            ),
            (
              "\"-1.5\"^^xsd:float",
              "\"1.0\"^^xsd:numeric",
              "\"-1.5\"^^xsd:numeric"
            )
          ).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncArithmetics.multiply(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DOUBLE typed integer and second is numeric literal" in {

          val df = List(
            ("\"1.5\"^^xsd:double", "1.5", "\"2.25\"^^xsd:double"),
            ("\"1.0\"^^xsd:double", "\"1\"^^xsd:int", "\"1.0\"^^xsd:double"),
            (
              "\"1.0\"^^xsd:double",
              "\"-1\"^^xsd:integer",
              "\"-1.0\"^^xsd:double"
            ),
            (
              "\"1.5\"^^xsd:double",
              "\"2\"^^xsd:decimal",
              "\"3.0\"^^xsd:double"
            ),
            (
              "\"-1.5\"^^xsd:double",
              "\"1.5\"^^xsd:float",
              "\"-2.25\"^^xsd:double"
            ),
            (
              "\"-1.5\"^^xsd:double",
              "\"-1.5\"^^xsd:double",
              "\"2.25\"^^xsd:double"
            ),
            (
              "\"1.5\"^^xsd:double",
              "\"1.0\"^^xsd:numeric",
              "\"1.5\"^^xsd:numeric"
            )
          ).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncArithmetics.multiply(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is NUMERIC typed integer and second is numeric literal" in {

          val df = List(
            ("\"1.5\"^^xsd:numeric", "1.5", "\"2.25\"^^xsd:numeric"),
            (
              "\"1.5\"^^xsd:numeric",
              "\"-1\"^^xsd:int",
              "\"-1.5\"^^xsd:numeric"
            ),
            (
              "\"-1.5\"^^xsd:numeric",
              "\"1\"^^xsd:integer",
              "\"-1.5\"^^xsd:numeric"
            ),
            (
              "\"-1.5\"^^xsd:numeric",
              "\"-1\"^^xsd:decimal",
              "\"1.5\"^^xsd:numeric"
            ),
            (
              "\"1.5\"^^xsd:numeric",
              "\"2.0\"^^xsd:float",
              "\"3.0\"^^xsd:numeric"
            ),
            (
              "\"1.5\"^^xsd:numeric",
              "\"-2.0\"^^xsd:double",
              "\"-3.0\"^^xsd:double"
            ),
            (
              "\"-1.5\"^^xsd:numeric",
              "\"2.0\"^^xsd:numeric",
              "\"-3.0\"^^xsd:numeric"
            )
          ).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncArithmetics.multiply(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }
      }
    }

    "FuncArithmetics.divide" should {

      "operate on numbers correctly" when {

        "first argument is plain literal and second is numeric literal" in {

          val df = List(
            ("1", "1", "1.0"),
            ("0.0", "1.0", "0.0"),
            ("1.0", "0.0", null),
            ("2", "\"1\"^^xsd:int", "\"2\"^^xsd:int"),
            ("4", "\"2\"^^xsd:integer", "\"2\"^^xsd:integer"),
            ("6", "\"2\"^^xsd:decimal", "\"3\"^^xsd:decimal"),
            ("3.0", "\"1.5\"^^xsd:float", "\"2.0\"^^xsd:float"),
            ("2.22", "\"2.0\"^^xsd:double", "\"1.11\"^^xsd:double"),
            ("5.0", "\"2.5\"^^xsd:numeric", "\"2.0\"^^xsd:numeric")
          ).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncArithmetics.divide(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INT typed integer and second is numeric literal" in {

          val df = List(
            ("\"1\"^^xsd:int", "1", "\"1\"^^xsd:int"),
            ("\"1\"^^xsd:int", "3.0", "\"0\"^^xsd:int"),
            ("\"2\"^^xsd:int", "\"1\"^^xsd:int", "\"2\"^^xsd:int"),
            ("\"4\"^^xsd:int", "\"2\"^^xsd:integer", "\"2\"^^xsd:integer"),
            ("\"6\"^^xsd:int", "\"2\"^^xsd:decimal", "\"3\"^^xsd:decimal"),
            ("\"-3\"^^xsd:int", "\"1.5\"^^xsd:float", "\"-2.0\"^^xsd:float"),
            ("\"-4\"^^xsd:int", "\"-1.25\"^^xsd:double", "\"3.2\"^^xsd:double"),
            ("\"-6\"^^xsd:int", "\"-1.5\"^^xsd:numeric", "\"4.0\"^^xsd:numeric")
          ).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncArithmetics.divide(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INTEGER typed integer and second is numeric literal" in {

          val df = List(
            ("\"1\"^^xsd:integer", "1", "\"1\"^^xsd:integer"),
            ("\"1\"^^xsd:integer", "3.0", "\"0\"^^xsd:integer"),
            ("\"2\"^^xsd:integer", "\"1\"^^xsd:int", "\"2\"^^xsd:int"),
            ("\"4\"^^xsd:integer", "\"2\"^^xsd:integer", "\"2\"^^xsd:integer"),
            ("\"6\"^^xsd:integer", "\"2\"^^xsd:decimal", "\"3\"^^xsd:decimal"),
            (
              "\"-3\"^^xsd:integer",
              "\"1.5\"^^xsd:float",
              "\"-2.0\"^^xsd:float"
            ),
            (
              "\"-4\"^^xsd:integer",
              "\"-1.25\"^^xsd:double",
              "\"3.2\"^^xsd:double"
            ),
            (
              "\"-6\"^^xsd:integer",
              "\"-1.5\"^^xsd:numeric",
              "\"4.0\"^^xsd:numeric"
            )
          ).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncArithmetics.divide(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DECIMAL typed integer and second is numeric literal" in {

          val df = List(
            ("\"1\"^^xsd:decimal", "1", "\"1\"^^xsd:decimal"),
            ("\"1\"^^xsd:decimal", "3.0", "\"0\"^^xsd:decimal"),
            ("\"2\"^^xsd:decimal", "\"1\"^^xsd:int", "\"2\"^^xsd:decimal"),
            ("\"4\"^^xsd:decimal", "\"2\"^^xsd:integer", "\"2\"^^xsd:decimal"),
            ("\"6\"^^xsd:decimal", "\"2\"^^xsd:decimal", "\"3\"^^xsd:decimal"),
            (
              "\"-3\"^^xsd:decimal",
              "\"1.5\"^^xsd:float",
              "\"-2.0\"^^xsd:float"
            ),
            (
              "\"-4\"^^xsd:decimal",
              "\"-1.25\"^^xsd:double",
              "\"3.2\"^^xsd:double"
            ),
            (
              "\"-6\"^^xsd:decimal",
              "\"-1.5\"^^xsd:numeric",
              "\"4.0\"^^xsd:numeric"
            )
          ).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncArithmetics.divide(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is FLOAT typed integer and second is numeric literal" in {

          val df = List(
            ("\"1.0\"^^xsd:float", "1", "\"1.0\"^^xsd:float"),
            ("\"1.0\"^^xsd:float", "3.0", "\"0.3333333333333333\"^^xsd:float"),
            ("\"2.0\"^^xsd:float", "\"1\"^^xsd:int", "\"2.0\"^^xsd:float"),
            ("\"4.0\"^^xsd:float", "\"2\"^^xsd:integer", "\"2.0\"^^xsd:float"),
            ("\"6.0\"^^xsd:float", "\"2\"^^xsd:decimal", "\"3.0\"^^xsd:float"),
            (
              "\"-3.0\"^^xsd:float",
              "\"1.5\"^^xsd:float",
              "\"-2.0\"^^xsd:float"
            ),
            (
              "\"-4.0\"^^xsd:float",
              "\"-1.25\"^^xsd:double",
              "\"3.2\"^^xsd:double"
            ),
            (
              "\"-6.0\"^^xsd:float",
              "\"-1.5\"^^xsd:numeric",
              "\"4.0\"^^xsd:numeric"
            )
          ).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncArithmetics.divide(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DOUBLE typed integer and second is numeric literal" in {

          val df = List(
            ("\"1.0\"^^xsd:double", "1", "\"1.0\"^^xsd:double"),
            (
              "\"1.0\"^^xsd:double",
              "3.0",
              "\"0.3333333333333333\"^^xsd:double"
            ),
            ("\"2.0\"^^xsd:double", "\"1\"^^xsd:int", "\"2.0\"^^xsd:double"),
            (
              "\"4.0\"^^xsd:double",
              "\"2\"^^xsd:integer",
              "\"2.0\"^^xsd:double"
            ),
            (
              "\"6.0\"^^xsd:double",
              "\"2\"^^xsd:decimal",
              "\"3.0\"^^xsd:double"
            ),
            (
              "\"-3.0\"^^xsd:double",
              "\"1.5\"^^xsd:float",
              "\"-2.0\"^^xsd:double"
            ),
            (
              "\"-4.0\"^^xsd:double",
              "\"-1.25\"^^xsd:double",
              "\"3.2\"^^xsd:double"
            ),
            (
              "\"-6.0\"^^xsd:double",
              "\"-1.5\"^^xsd:numeric",
              "\"4.0\"^^xsd:numeric"
            )
          ).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncArithmetics.divide(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is NUMERIC typed integer and second is numeric literal" in {

          val df = List(
            ("\"1.0\"^^xsd:numeric", "1", "\"1.0\"^^xsd:numeric"),
            (
              "\"1.0\"^^xsd:numeric",
              "3.0",
              "\"0.3333333333333333\"^^xsd:numeric"
            ),
            ("\"2.0\"^^xsd:numeric", "\"1\"^^xsd:int", "\"2.0\"^^xsd:numeric"),
            (
              "\"4.0\"^^xsd:numeric",
              "\"2\"^^xsd:integer",
              "\"2.0\"^^xsd:numeric"
            ),
            (
              "\"6.0\"^^xsd:numeric",
              "\"2\"^^xsd:decimal",
              "\"3.0\"^^xsd:numeric"
            ),
            (
              "\"-3.0\"^^xsd:numeric",
              "\"1.5\"^^xsd:float",
              "\"-2.0\"^^xsd:numeric"
            ),
            (
              "\"-4.0\"^^xsd:numeric",
              "\"-1.25\"^^xsd:double",
              "\"3.2\"^^xsd:double"
            ),
            (
              "\"-6.0\"^^xsd:numeric",
              "\"-1.5\"^^xsd:numeric",
              "\"4.0\"^^xsd:numeric"
            )
          ).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncArithmetics.divide(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }
      }
    }
  }
}
