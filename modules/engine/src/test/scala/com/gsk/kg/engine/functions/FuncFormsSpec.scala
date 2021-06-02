package com.gsk.kg.engine.functions

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

import com.gsk.kg.engine.compiler.SparkSpec
import com.gsk.kg.engine.scalacheck.CommonGenerators

import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAccessor

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class FuncFormsSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with ScalaCheckDrivenPropertyChecks
    with CommonGenerators {

  import sqlContext.implicits._

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  "Funcs on Forms" when {

    "FuncForms.equals" should {

      "operates on numbers correctly" when {

        "first argument is not typed number and second is other number type" in {
          val plainNumberCorrectCases = List(
            ("1", "1", true),
            ("1", "\"1\"^^xsd:int", true),
            ("1", "\"1\"^^xsd:integer", true),
            ("1", "\"1\"^^xsd:decimal", true),
            ("1.23", "\"1.23\"^^xsd:float", true),
            ("1.23", "\"1.23\"^^xsd:double", true),
            ("1.23", "\"1.23\"^^xsd:numeric", true)
          )

          val plainNumberWrongCases = List(
            ("1", "2", false),
            ("1", "\"2\"^^xsd:int", false),
            ("1", "\"2\"^^xsd:integer", false),
            ("1", "\"0\"^^xsd:decimal", false),
            ("1.23", "\"1.1\"^^xsd:float", false),
            ("1.23", "\"1.1\"^^xsd:double", false),
            ("1.23", "\"1.1\"^^xsd:numeric", false)
          )

          val df = (plainNumberCorrectCases ++ plainNumberWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncForms.equals(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INT type number and second is other number type" in {

          val intCorrectCases = List(
            ("\"1\"^^xsd:int", "1", true),
            ("\"1\"^^xsd:int", "1.0", true),
            ("\"1\"^^xsd:int", "\"1\"^^xsd:int", true),
            ("\"1\"^^xsd:int", "\"1\"^^xsd:integer", true),
            ("\"1\"^^xsd:int", "\"1\"^^xsd:decimal", true),
            ("\"1\"^^xsd:int", "\"1.0\"^^xsd:float", true),
            ("\"1\"^^xsd:int", "\"1.0\"^^xsd:double", true),
            ("\"1\"^^xsd:int", "\"1.0\"^^xsd:numeric", true)
          )

          val intWrongCases = List(
            ("\"1\"^^xsd:int", "2", false),
            ("\"1\"^^xsd:int", "1.1", false),
            ("\"1\"^^xsd:int", "\"2\"^^xsd:int", false),
            ("\"1\"^^xsd:int", "\"2\"^^xsd:integer", false),
            ("\"1\"^^xsd:int", "\"0\"^^xsd:decimal", false),
            ("\"1\"^^xsd:int", "\"1.1\"^^xsd:float", false),
            ("\"1\"^^xsd:int", "\"1.1\"^^xsd:double", false),
            ("\"1\"^^xsd:int", "\"1.1\"^^xsd:numeric", false)
          )

          val df = (intCorrectCases ++ intWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncForms.equals(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INTEGER type number and second is other number type" in {

          val integerCorrectCases = List(
            ("\"1\"^^xsd:integer", "1", true),
            ("\"1\"^^xsd:integer", "1.0", true),
            ("\"1\"^^xsd:integer", "\"1\"^^xsd:int", true),
            ("\"1\"^^xsd:integer", "\"1\"^^xsd:integer", true),
            ("\"1\"^^xsd:integer", "\"1\"^^xsd:decimal", true),
            ("\"1\"^^xsd:integer", "\"1.0\"^^xsd:float", true),
            ("\"1\"^^xsd:integer", "\"1.0\"^^xsd:double", true),
            ("\"1\"^^xsd:integer", "\"1.0\"^^xsd:numeric", true)
          )

          val integerWrongCases = List(
            ("\"1\"^^xsd:integer", "2", false),
            ("\"1\"^^xsd:integer", "1.1", false),
            ("\"1\"^^xsd:integer", "\"2\"^^xsd:int", false),
            ("\"1\"^^xsd:integer", "\"2\"^^xsd:integer", false),
            ("\"1\"^^xsd:integer", "\"0\"^^xsd:decimal", false),
            ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:float", false),
            ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:double", false),
            ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:numeric", false)
          )

          val df = (integerCorrectCases ++ integerWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncForms.equals(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DECIMAL type number and second is other number type" in {

          val decimalCorrectCases = List(
            ("\"1\"^^xsd:decimal", "1", true),
            ("\"1\"^^xsd:decimal", "1.0", true),
            ("\"1\"^^xsd:decimal", "\"1\"^^xsd:int", true),
            ("\"1\"^^xsd:decimal", "\"1\"^^xsd:integer", true),
            ("\"1\"^^xsd:decimal", "\"1\"^^xsd:decimal", true),
            ("\"1\"^^xsd:decimal", "\"1.0\"^^xsd:float", true),
            ("\"1\"^^xsd:decimal", "\"1.0\"^^xsd:double", true),
            ("\"1\"^^xsd:decimal", "\"1.0\"^^xsd:numeric", true)
          )

          val decimalWrongCases = List(
            ("\"1\"^^xsd:decimal", "2", false),
            ("\"1\"^^xsd:decimal", "1.1", false),
            ("\"1\"^^xsd:decimal", "\"2\"^^xsd:int", false),
            ("\"1\"^^xsd:decimal", "\"2\"^^xsd:integer", false),
            ("\"1\"^^xsd:decimal", "\"2\"^^xsd:decimal", false),
            ("\"1\"^^xsd:decimal", "\"1.24\"^^xsd:float", false),
            ("\"1\"^^xsd:decimal", "\"1.24\"^^xsd:double", false),
            ("\"1\"^^xsd:decimal", "\"1.24\"^^xsd:numeric", false)
          )

          val df = (decimalCorrectCases ++ decimalWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncForms.equals(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is FLOAT type number and second is other number type" in {

          val floatCorrectCases = List(
            ("\"1.23\"^^xsd:float", "1.23", true),
            ("\"1.0\"^^xsd:float", "\"1\"^^xsd:int", true),
            ("\"1.0\"^^xsd:float", "\"1\"^^xsd:integer", true),
            ("\"1.0\"^^xsd:float", "\"1\"^^xsd:decimal", true),
            ("\"1.23\"^^xsd:float", "\"1.23\"^^xsd:float", true),
            ("\"1.23\"^^xsd:float", "\"1.23\"^^xsd:double", true),
            ("\"1.23\"^^xsd:float", "\"1.23\"^^xsd:numeric", true)
          )

          val floatWrongCases = List(
            ("\"1.23\"^^xsd:float", "1.24", false),
            ("\"1.23\"^^xsd:float", "\"1\"^^xsd:int", false),
            ("\"1.23\"^^xsd:float", "\"1\"^^xsd:integer", false),
            ("\"1.23\"^^xsd:float", "\"1\"^^xsd:decimal", false),
            ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:float", false),
            ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:double", false),
            ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:numeric", false)
          )

          val df = (floatCorrectCases ++ floatWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncForms.equals(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DOUBLE type number and second is other number type" in {

          val doubleCorrectCases = List(
            ("\"1.23\"^^xsd:double", "1.23", true),
            ("\"1.0\"^^xsd:double", "\"1\"^^xsd:int", true),
            ("\"1.0\"^^xsd:double", "\"1\"^^xsd:integer", true),
            ("\"1.23\"^^xsd:double", "\"1.23\"^^xsd:decimal", true),
            ("\"1.23\"^^xsd:double", "\"1.23\"^^xsd:float", true),
            ("\"1.23\"^^xsd:double", "\"1.23\"^^xsd:double", true),
            ("\"1.23\"^^xsd:double", "\"1.23\"^^xsd:numeric", true)
          )

          val doubleWrongCases = List(
            ("\"1.23\"^^xsd:double", "1.24", false),
            ("\"1.23\"^^xsd:double", "\"1\"^^xsd:int", false),
            ("\"1.23\"^^xsd:double", "\"1\"^^xsd:integer", false),
            ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:decimal", false),
            ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:float", false),
            ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:double", false),
            ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:numeric", false)
          )

          val df = (doubleCorrectCases ++ doubleWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncForms.equals(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is NUMERIC type number and second is other number type" in {

          val numericCorrectCases = List(
            ("\"1.23\"^^xsd:numeric", "1.23", true),
            ("\"1.0\"^^xsd:numeric", "\"1\"^^xsd:int", true),
            ("\"1.0\"^^xsd:numeric", "\"1\"^^xsd:integer", true),
            ("\"1.23\"^^xsd:numeric", "\"1.23\"^^xsd:decimal", true),
            ("\"1.23\"^^xsd:numeric", "\"1.23\"^^xsd:float", true),
            ("\"1.23\"^^xsd:numeric", "\"1.23\"^^xsd:double", true),
            ("\"1.23\"^^xsd:numeric", "\"1.23\"^^xsd:numeric", true)
          )

          val numericWrongCases = List(
            ("\"1.23\"^^xsd:numeric", "1.24", false),
            ("\"1.23\"^^xsd:numeric", "\"1\"^^xsd:int", false),
            ("\"1.23\"^^xsd:numeric", "\"1\"^^xsd:integer", false),
            ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:decimal", false),
            ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:float", false),
            ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:double", false),
            ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:numeric", false)
          )

          val df = (numericCorrectCases ++ numericWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncForms.equals(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }
      }

      "operate on equal dates correctly" in {

        forAll { datetime: LocalDateTime =>
          val df = List(
            (
              toRDFDateTime(datetime),
              toRDFDateTime(datetime)
            )
          ).toDF("a", "b")

          df.select(FuncForms.equals(df("a"), df("b")))
            .collect() shouldEqual Array(
            Row(true)
          )
        }
      }

      "operate on different dates correctly" in {

        forAll { datetime: LocalDateTime =>
          val df = List(
            (
              toRDFDateTime(datetime.plusSeconds(1)),
              toRDFDateTime(datetime)
            )
          ).toDF("a", "b")

          df.select(FuncForms.equals(df("a"), df("b")))
            .collect() shouldEqual Array(
            Row(false)
          )
        }
      }
    }

    "FuncForms.gt" should {

      "operates on numbers correctly" when {

        "first argument is not typed number and second is other number type" in {
          val plainNumberCorrectCases = List(
            ("2", "1", true),
            ("2", "\"1\"^^xsd:int", true),
            ("2", "\"1\"^^xsd:integer", true),
            ("2", "\"1\"^^xsd:decimal", true),
            ("1.24", "\"1.23\"^^xsd:float", true),
            ("1.24", "\"1.23\"^^xsd:double", true),
            ("1.24", "\"1.23\"^^xsd:numeric", true)
          )

          val plainNumberWrongCases = List(
            ("1", "2", false),
            ("1", "\"2\"^^xsd:int", false),
            ("1", "\"2\"^^xsd:integer", false),
            ("1", "\"2\"^^xsd:decimal", false),
            ("1.23", "\"1.24\"^^xsd:float", false),
            ("1.23", "\"1.24\"^^xsd:double", false),
            ("1.23", "\"1.24\"^^xsd:numeric", false)
          )

          val df = (plainNumberCorrectCases ++ plainNumberWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncForms.gt(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INT type number and second is other number type" in {

          val intCorrectCases = List(
            ("\"2\"^^xsd:int", "1", true),
            ("\"2\"^^xsd:int", "1.0", true),
            ("\"2\"^^xsd:int", "\"1\"^^xsd:int", true),
            ("\"2\"^^xsd:int", "\"1\"^^xsd:integer", true),
            ("\"2\"^^xsd:int", "\"1\"^^xsd:decimal", true),
            ("\"2\"^^xsd:int", "\"1.0\"^^xsd:float", true),
            ("\"2\"^^xsd:int", "\"1.0\"^^xsd:double", true),
            ("\"2\"^^xsd:int", "\"1.0\"^^xsd:numeric", true)
          )

          val intWrongCases = List(
            ("\"1\"^^xsd:int", "2", false),
            ("\"1\"^^xsd:int", "1.1", false),
            ("\"1\"^^xsd:int", "\"2\"^^xsd:int", false),
            ("\"1\"^^xsd:int", "\"2\"^^xsd:integer", false),
            ("\"1\"^^xsd:int", "\"2\"^^xsd:decimal", false),
            ("\"1\"^^xsd:int", "\"1.1\"^^xsd:float", false),
            ("\"1\"^^xsd:int", "\"1.1\"^^xsd:double", false),
            ("\"1\"^^xsd:int", "\"1.1\"^^xsd:numeric", false)
          )

          val df = (intCorrectCases ++ intWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncForms.gt(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INTEGER type number and second is other number type" in {

          val integerCorrectCases = List(
            ("\"2\"^^xsd:integer", "1", true),
            ("\"2\"^^xsd:integer", "1.0", true),
            ("\"2\"^^xsd:integer", "\"1\"^^xsd:int", true),
            ("\"2\"^^xsd:integer", "\"1\"^^xsd:integer", true),
            ("\"2\"^^xsd:integer", "\"1\"^^xsd:decimal", true),
            ("\"2\"^^xsd:integer", "\"1.0\"^^xsd:float", true),
            ("\"2\"^^xsd:integer", "\"1.0\"^^xsd:double", true),
            ("\"2\"^^xsd:integer", "\"1.0\"^^xsd:numeric", true)
          )

          val integerWrongCases = List(
            ("\"1\"^^xsd:integer", "2", false),
            ("\"1\"^^xsd:integer", "1.1", false),
            ("\"1\"^^xsd:integer", "\"2\"^^xsd:int", false),
            ("\"1\"^^xsd:integer", "\"2\"^^xsd:integer", false),
            ("\"1\"^^xsd:integer", "\"2\"^^xsd:decimal", false),
            ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:float", false),
            ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:double", false),
            ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:numeric", false)
          )

          val df = (integerCorrectCases ++ integerWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncForms.gt(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DECIMAL type number and second is other number type" in {

          val decimalCorrectCases = List(
            ("\"2\"^^xsd:decimal", "1", true),
            ("\"2\"^^xsd:decimal", "1.0", true),
            ("\"2\"^^xsd:decimal", "\"1\"^^xsd:int", true),
            ("\"2\"^^xsd:decimal", "\"1\"^^xsd:integer", true),
            ("\"2\"^^xsd:decimal", "\"1\"^^xsd:decimal", true),
            ("\"2\"^^xsd:decimal", "\"1.0\"^^xsd:float", true),
            ("\"2\"^^xsd:decimal", "\"1.0\"^^xsd:double", true),
            ("\"2\"^^xsd:decimal", "\"1.0\"^^xsd:numeric", true)
          )

          val decimalWrongCases = List(
            ("\"1\"^^xsd:decimal", "2", false),
            ("\"1\"^^xsd:decimal", "1.1", false),
            ("\"1\"^^xsd:decimal", "\"2\"^^xsd:int", false),
            ("\"1\"^^xsd:decimal", "\"2\"^^xsd:integer", false),
            ("\"1\"^^xsd:decimal", "\"2\"^^xsd:decimal", false),
            ("\"1\"^^xsd:decimal", "\"1.24\"^^xsd:float", false),
            ("\"1\"^^xsd:decimal", "\"1.24\"^^xsd:double", false),
            ("\"1\"^^xsd:decimal", "\"1.24\"^^xsd:numeric", false)
          )

          val df = (decimalCorrectCases ++ decimalWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncForms.gt(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is FLOAT type number and second is other number type" in {

          val floatCorrectCases = List(
            ("\"1.23\"^^xsd:float", "1.22", true),
            ("\"1.1\"^^xsd:float", "\"1\"^^xsd:int", true),
            ("\"1.1\"^^xsd:float", "\"1\"^^xsd:integer", true),
            ("\"1.1\"^^xsd:float", "\"1\"^^xsd:decimal", true),
            ("\"1.23\"^^xsd:float", "\"1.22\"^^xsd:float", true),
            ("\"1.23\"^^xsd:float", "\"1.22\"^^xsd:double", true),
            ("\"1.23\"^^xsd:float", "\"1.22\"^^xsd:numeric", true)
          )

          val floatWrongCases = List(
            ("\"1.23\"^^xsd:float", "1.24", false),
            ("\"1.23\"^^xsd:float", "\"2\"^^xsd:int", false),
            ("\"1.23\"^^xsd:float", "\"2\"^^xsd:integer", false),
            ("\"1.23\"^^xsd:float", "\"2\"^^xsd:decimal", false),
            ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:float", false),
            ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:double", false),
            ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:numeric", false)
          )

          val df = (floatCorrectCases ++ floatWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncForms.gt(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DOUBLE type number and second is other number type" in {

          val doubleCorrectCases = List(
            ("\"1.23\"^^xsd:double", "1.22", true),
            ("\"1.1\"^^xsd:double", "\"1\"^^xsd:int", true),
            ("\"1.1\"^^xsd:double", "\"1\"^^xsd:integer", true),
            ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:decimal", true),
            ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:float", true),
            ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:double", true),
            ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:numeric", true)
          )

          val doubleWrongCases = List(
            ("\"1.23\"^^xsd:double", "1.24", false),
            ("\"1.23\"^^xsd:double", "\"2\"^^xsd:int", false),
            ("\"1.23\"^^xsd:double", "\"2\"^^xsd:integer", false),
            ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:decimal", false),
            ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:float", false),
            ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:double", false),
            ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:numeric", false)
          )

          val df = (doubleCorrectCases ++ doubleWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncForms.gt(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is NUMERIC type number and second is other number type" in {

          val numericCorrectCases = List(
            ("\"1.23\"^^xsd:numeric", "1.22", true),
            ("\"1.23\"^^xsd:numeric", "\"1\"^^xsd:int", true),
            ("\"1.23\"^^xsd:numeric", "\"1\"^^xsd:integer", true),
            ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:decimal", true),
            ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:float", true),
            ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:double", true),
            ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:numeric", true)
          )

          val numericWrongCases = List(
            ("\"1.23\"^^xsd:numeric", "1.24", false),
            ("\"1.23\"^^xsd:numeric", "\"2\"^^xsd:int", false),
            ("\"1.23\"^^xsd:numeric", "\"2\"^^xsd:integer", false),
            ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:decimal", false),
            ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:float", false),
            ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:double", false),
            ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:numeric", false)
          )

          val df = (numericCorrectCases ++ numericWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncForms.gt(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }
      }

      "work for integer values" in {

        val df = List(
          (2, 1)
        ).toDF("a", "b")

        df.select(FuncForms.gt(df("a"), df("b"))).collect() shouldEqual Array(
          Row(true)
        )
      }

      "work in datetimes without a zone" in {

        forAll { datetime: LocalDateTime =>
          val df = List(
            (
              toRDFDateTime(datetime.plusSeconds(1)),
              toRDFDateTime(datetime)
            )
          ).toDF("a", "b")

          df.select(FuncForms.gt(df("a"), df("b"))).collect() shouldEqual Array(
            Row(true)
          )
        }
      }

      "work in datetimes with zone" in {

        forAll { datetime: LocalDateTime =>
          val df = List(
            (
              toRDFDateTime(OffsetDateTime.of(datetime, ZoneOffset.ofHours(4))),
              toRDFDateTime(OffsetDateTime.of(datetime, ZoneOffset.ofHours(5)))
            )
          ).toDF("a", "b")

          df.select(
            FuncForms.gt(df("a"), df("b"))
          ).collect() shouldEqual Array(
            Row(true)
          )
        }
      }
    }

    "FuncForms.lt" should {

      "operates on numbers correctly" when {

        "first argument is not typed number and second is other number type" in {
          val plainNumberCorrectCases = List(
            ("1", "2", true),
            ("1", "\"2\"^^xsd:int", true),
            ("1", "\"2\"^^xsd:integer", true),
            ("1", "\"2\"^^xsd:decimal", true),
            ("1.23", "\"1.24\"^^xsd:float", true),
            ("1.23", "\"1.24\"^^xsd:double", true),
            ("1.23", "\"1.24\"^^xsd:numeric", true)
          )

          val plainNumberWrongCases = List(
            ("2", "1", false),
            ("2", "\"1\"^^xsd:int", false),
            ("2", "\"1\"^^xsd:integer", false),
            ("2", "\"1\"^^xsd:decimal", false),
            ("1.24", "\"1.23\"^^xsd:float", false),
            ("1.24", "\"1.23\"^^xsd:double", false),
            ("1.24", "\"1.23\"^^xsd:numeric", false)
          )

          val df = (plainNumberCorrectCases ++ plainNumberWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncForms.lt(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INT type number and second is other number type" in {

          val intCorrectCases = List(
            ("\"1\"^^xsd:int", "2", true),
            ("\"1\"^^xsd:int", "1.1", true),
            ("\"1\"^^xsd:int", "\"2\"^^xsd:int", true),
            ("\"1\"^^xsd:int", "\"2\"^^xsd:integer", true),
            ("\"1\"^^xsd:int", "\"2\"^^xsd:decimal", true),
            ("\"1\"^^xsd:int", "\"1.1\"^^xsd:float", true),
            ("\"1\"^^xsd:int", "\"1.1\"^^xsd:double", true),
            ("\"1\"^^xsd:int", "\"1.1\"^^xsd:numeric", true)
          )

          val intWrongCases = List(
            ("\"2\"^^xsd:int", "1", false),
            ("\"2\"^^xsd:int", "1.1", false),
            ("\"2\"^^xsd:int", "\"1\"^^xsd:int", false),
            ("\"2\"^^xsd:int", "\"1\"^^xsd:integer", false),
            ("\"2\"^^xsd:int", "\"1\"^^xsd:decimal", false),
            ("\"2\"^^xsd:int", "\"1.1\"^^xsd:float", false),
            ("\"2\"^^xsd:int", "\"1.1\"^^xsd:double", false),
            ("\"2\"^^xsd:int", "\"1.1\"^^xsd:numeric", false)
          )

          val df = (intCorrectCases ++ intWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncForms.lt(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INTEGER type number and second is other number type" in {

          val integerCorrectCases = List(
            ("\"1\"^^xsd:integer", "2", true),
            ("\"1\"^^xsd:integer", "1.1", true),
            ("\"1\"^^xsd:integer", "\"2\"^^xsd:int", true),
            ("\"1\"^^xsd:integer", "\"2\"^^xsd:integer", true),
            ("\"1\"^^xsd:integer", "\"2\"^^xsd:decimal", true),
            ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:float", true),
            ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:double", true),
            ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:numeric", true)
          )

          val integerWrongCases = List(
            ("\"2\"^^xsd:integer", "1", false),
            ("\"2\"^^xsd:integer", "1.1", false),
            ("\"2\"^^xsd:integer", "\"1\"^^xsd:int", false),
            ("\"2\"^^xsd:integer", "\"1\"^^xsd:integer", false),
            ("\"2\"^^xsd:integer", "\"1\"^^xsd:decimal", false),
            ("\"2\"^^xsd:integer", "\"1.1\"^^xsd:float", false),
            ("\"2\"^^xsd:integer", "\"1.1\"^^xsd:double", false),
            ("\"2\"^^xsd:integer", "\"1.1\"^^xsd:numeric", false)
          )

          val df = (integerCorrectCases ++ integerWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncForms.lt(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DECIMAL type number and second is other number type" in {

          val decimalCorrectCases = List(
            ("\"1\"^^xsd:decimal", "2", true),
            ("\"1\"^^xsd:decimal", "1.1", true),
            ("\"1\"^^xsd:decimal", "\"2\"^^xsd:int", true),
            ("\"1\"^^xsd:decimal", "\"2\"^^xsd:integer", true),
            ("\"1\"^^xsd:decimal", "\"2\"^^xsd:decimal", true),
            ("\"1\"^^xsd:decimal", "\"1.1\"^^xsd:float", true),
            ("\"1\"^^xsd:decimal", "\"1.1\"^^xsd:double", true),
            ("\"1\"^^xsd:decimal", "\"1.1\"^^xsd:numeric", true)
          )

          val decimalWrongCases = List(
            ("\"2\"^^xsd:decimal", "1", false),
            ("\"2\"^^xsd:decimal", "1.1", false),
            ("\"2\"^^xsd:decimal", "\"1\"^^xsd:int", false),
            ("\"2\"^^xsd:decimal", "\"1\"^^xsd:integer", false),
            ("\"2\"^^xsd:decimal", "\"1\"^^xsd:decimal", false),
            ("\"2\"^^xsd:decimal", "\"1.24\"^^xsd:float", false),
            ("\"2\"^^xsd:decimal", "\"1.24\"^^xsd:double", false),
            ("\"2\"^^xsd:decimal", "\"1.24\"^^xsd:numeric", false)
          )

          val df = (decimalCorrectCases ++ decimalWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncForms.lt(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is FLOAT type number and second is other number type" in {

          val floatCorrectCases = List(
            ("\"1.23\"^^xsd:float", "1.24", true),
            ("\"1.1\"^^xsd:float", "\"2\"^^xsd:int", true),
            ("\"1.1\"^^xsd:float", "\"2\"^^xsd:integer", true),
            ("\"1.1\"^^xsd:float", "\"2\"^^xsd:decimal", true),
            ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:float", true),
            ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:double", true),
            ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:numeric", true)
          )

          val floatWrongCases = List(
            ("\"1.24\"^^xsd:float", "1.23", false),
            ("\"1.24\"^^xsd:float", "\"1\"^^xsd:int", false),
            ("\"1.24\"^^xsd:float", "\"1\"^^xsd:integer", false),
            ("\"1.24\"^^xsd:float", "\"1\"^^xsd:decimal", false),
            ("\"1.24\"^^xsd:float", "\"1.23\"^^xsd:float", false),
            ("\"1.24\"^^xsd:float", "\"1.23\"^^xsd:double", false),
            ("\"1.24\"^^xsd:float", "\"1.23\"^^xsd:numeric", false)
          )

          val df = (floatCorrectCases ++ floatWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncForms.lt(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DOUBLE type number and second is other number type" in {

          val doubleCorrectCases = List(
            ("\"1.23\"^^xsd:double", "1.24", true),
            ("\"1.1\"^^xsd:double", "\"2\"^^xsd:int", true),
            ("\"1.1\"^^xsd:double", "\"2\"^^xsd:integer", true),
            ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:decimal", true),
            ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:float", true),
            ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:double", true),
            ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:numeric", true)
          )

          val doubleWrongCases = List(
            ("\"1.23\"^^xsd:double", "1.22", false),
            ("\"1.23\"^^xsd:double", "\"1\"^^xsd:int", false),
            ("\"1.23\"^^xsd:double", "\"1\"^^xsd:integer", false),
            ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:decimal", false),
            ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:float", false),
            ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:double", false),
            ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:numeric", false)
          )

          val df = (doubleCorrectCases ++ doubleWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncForms.lt(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is NUMERIC type number and second is other number type" in {

          val numericCorrectCases = List(
            ("\"1.23\"^^xsd:numeric", "1.24", true),
            ("\"1.23\"^^xsd:numeric", "\"2\"^^xsd:int", true),
            ("\"1.23\"^^xsd:numeric", "\"2\"^^xsd:integer", true),
            ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:decimal", true),
            ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:float", true),
            ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:double", true),
            ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:numeric", true)
          )

          val numericWrongCases = List(
            ("\"1.23\"^^xsd:numeric", "1.22", false),
            ("\"1.23\"^^xsd:numeric", "\"1\"^^xsd:int", false),
            ("\"1.23\"^^xsd:numeric", "\"1\"^^xsd:integer", false),
            ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:decimal", false),
            ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:float", false),
            ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:double", false),
            ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:numeric", false)
          )

          val df = (numericCorrectCases ++ numericWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncForms.lt(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }
      }

      "work for integer values" in {

        val df = List(
          (1, 2)
        ).toDF("a", "b")

        df.select(FuncForms.lt(df("a"), df("b"))).collect() shouldEqual Array(
          Row(true)
        )
      }

      "work in datetimes without a zone" in {

        forAll { datetime: LocalDateTime =>
          val df = List(
            (
              toRDFDateTime(datetime),
              toRDFDateTime(datetime.plusSeconds(1))
            )
          ).toDF("a", "b")

          df.select(FuncForms.lt(df("a"), df("b"))).collect() shouldEqual Array(
            Row(true)
          )
        }
      }

      "work in datetimes with zone" in {

        forAll { datetime: LocalDateTime =>
          val df = List(
            (
              toRDFDateTime(OffsetDateTime.of(datetime, ZoneOffset.ofHours(5))),
              toRDFDateTime(OffsetDateTime.of(datetime, ZoneOffset.ofHours(4)))
            )
          ).toDF("a", "b")

          df.select(
            FuncForms.lt(df("a"), df("b"))
          ).collect() shouldEqual Array(
            Row(true)
          )
        }
      }
    }

    "FuncForms.gte" should {

      "operates on numbers correctly" when {

        "first argument is not typed number and second is other number type" in {
          val plainNumberCorrectCases = List(
            ("2", "1", true),
            ("2", "\"1\"^^xsd:int", true),
            ("2", "\"1\"^^xsd:integer", true),
            ("2", "\"1\"^^xsd:decimal", true),
            ("1.24", "\"1.23\"^^xsd:float", true),
            ("1.24", "\"1.23\"^^xsd:double", true),
            ("1.24", "\"1.23\"^^xsd:numeric", true)
          )

          val plainNumberWrongCases = List(
            ("1", "2", false),
            ("1", "\"2\"^^xsd:int", false),
            ("1", "\"2\"^^xsd:integer", false),
            ("1", "\"2\"^^xsd:decimal", false),
            ("1.23", "\"1.24\"^^xsd:float", false),
            ("1.23", "\"1.24\"^^xsd:double", false),
            ("1.23", "\"1.24\"^^xsd:numeric", false)
          )

          val df = (plainNumberCorrectCases ++ plainNumberWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncForms.gte(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INT type number and second is other number type" in {

          val intCorrectCases = List(
            ("\"2\"^^xsd:int", "1", true),
            ("\"2\"^^xsd:int", "1.0", true),
            ("\"2\"^^xsd:int", "\"1\"^^xsd:int", true),
            ("\"2\"^^xsd:int", "\"1\"^^xsd:integer", true),
            ("\"2\"^^xsd:int", "\"1\"^^xsd:decimal", true),
            ("\"2\"^^xsd:int", "\"1.0\"^^xsd:float", true),
            ("\"2\"^^xsd:int", "\"1.0\"^^xsd:double", true),
            ("\"2\"^^xsd:int", "\"1.0\"^^xsd:numeric", true)
          )

          val intWrongCases = List(
            ("\"1\"^^xsd:int", "2", false),
            ("\"1\"^^xsd:int", "1.1", false),
            ("\"1\"^^xsd:int", "\"2\"^^xsd:int", false),
            ("\"1\"^^xsd:int", "\"2\"^^xsd:integer", false),
            ("\"1\"^^xsd:int", "\"2\"^^xsd:decimal", false),
            ("\"1\"^^xsd:int", "\"1.1\"^^xsd:float", false),
            ("\"1\"^^xsd:int", "\"1.1\"^^xsd:double", false),
            ("\"1\"^^xsd:int", "\"1.1\"^^xsd:numeric", false)
          )

          val df = (intCorrectCases ++ intWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncForms.gte(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INTEGER type number and second is other number type" in {

          val integerCorrectCases = List(
            ("\"2\"^^xsd:integer", "1", true),
            ("\"2\"^^xsd:integer", "1.0", true),
            ("\"2\"^^xsd:integer", "\"1\"^^xsd:int", true),
            ("\"2\"^^xsd:integer", "\"1\"^^xsd:integer", true),
            ("\"2\"^^xsd:integer", "\"1\"^^xsd:decimal", true),
            ("\"2\"^^xsd:integer", "\"1.0\"^^xsd:float", true),
            ("\"2\"^^xsd:integer", "\"1.0\"^^xsd:double", true),
            ("\"2\"^^xsd:integer", "\"1.0\"^^xsd:numeric", true)
          )

          val integerWrongCases = List(
            ("\"1\"^^xsd:integer", "2", false),
            ("\"1\"^^xsd:integer", "1.1", false),
            ("\"1\"^^xsd:integer", "\"2\"^^xsd:int", false),
            ("\"1\"^^xsd:integer", "\"2\"^^xsd:integer", false),
            ("\"1\"^^xsd:integer", "\"2\"^^xsd:decimal", false),
            ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:float", false),
            ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:double", false),
            ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:numeric", false)
          )

          val df = (integerCorrectCases ++ integerWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncForms.gte(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DECIMAL type number and second is other number type" in {

          val decimalCorrectCases = List(
            ("\"2\"^^xsd:decimal", "1", true),
            ("\"2\"^^xsd:decimal", "1.0", true),
            ("\"2\"^^xsd:decimal", "\"1\"^^xsd:int", true),
            ("\"2\"^^xsd:decimal", "\"1\"^^xsd:integer", true),
            ("\"2\"^^xsd:decimal", "\"1\"^^xsd:decimal", true),
            ("\"2\"^^xsd:decimal", "\"1.0\"^^xsd:float", true),
            ("\"2\"^^xsd:decimal", "\"1.0\"^^xsd:double", true),
            ("\"2\"^^xsd:decimal", "\"1.0\"^^xsd:numeric", true)
          )

          val decimalWrongCases = List(
            ("\"1\"^^xsd:decimal", "2", false),
            ("\"1\"^^xsd:decimal", "1.1", false),
            ("\"1\"^^xsd:decimal", "\"2\"^^xsd:int", false),
            ("\"1\"^^xsd:decimal", "\"2\"^^xsd:integer", false),
            ("\"1\"^^xsd:decimal", "\"2\"^^xsd:decimal", false),
            ("\"1\"^^xsd:decimal", "\"1.24\"^^xsd:float", false),
            ("\"1\"^^xsd:decimal", "\"1.24\"^^xsd:double", false),
            ("\"1\"^^xsd:decimal", "\"1.24\"^^xsd:numeric", false)
          )

          val df = (decimalCorrectCases ++ decimalWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncForms.gte(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is FLOAT type number and second is other number type" in {

          val floatCorrectCases = List(
            ("\"1.23\"^^xsd:float", "1.22", true),
            ("\"1.1\"^^xsd:float", "\"1\"^^xsd:int", true),
            ("\"1.1\"^^xsd:float", "\"1\"^^xsd:integer", true),
            ("\"1.1\"^^xsd:float", "\"1\"^^xsd:decimal", true),
            ("\"1.23\"^^xsd:float", "\"1.22\"^^xsd:float", true),
            ("\"1.23\"^^xsd:float", "\"1.22\"^^xsd:double", true),
            ("\"1.23\"^^xsd:float", "\"1.22\"^^xsd:numeric", true)
          )

          val floatWrongCases = List(
            ("\"1.23\"^^xsd:float", "1.24", false),
            ("\"1.23\"^^xsd:float", "\"2\"^^xsd:int", false),
            ("\"1.23\"^^xsd:float", "\"2\"^^xsd:integer", false),
            ("\"1.23\"^^xsd:float", "\"2\"^^xsd:decimal", false),
            ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:float", false),
            ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:double", false),
            ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:numeric", false)
          )

          val df = (floatCorrectCases ++ floatWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncForms.gte(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DOUBLE type number and second is other number type" in {

          val doubleCorrectCases = List(
            ("\"1.23\"^^xsd:double", "1.22", true),
            ("\"1.1\"^^xsd:double", "\"1\"^^xsd:int", true),
            ("\"1.1\"^^xsd:double", "\"1\"^^xsd:integer", true),
            ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:decimal", true),
            ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:float", true),
            ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:double", true),
            ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:numeric", true)
          )

          val doubleWrongCases = List(
            ("\"1.23\"^^xsd:double", "1.24", false),
            ("\"1.23\"^^xsd:double", "\"2\"^^xsd:int", false),
            ("\"1.23\"^^xsd:double", "\"2\"^^xsd:integer", false),
            ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:decimal", false),
            ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:float", false),
            ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:double", false),
            ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:numeric", false)
          )

          val df = (doubleCorrectCases ++ doubleWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncForms.gte(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is NUMERIC type number and second is other number type" in {

          val numericCorrectCases = List(
            ("\"1.23\"^^xsd:numeric", "1.22", true),
            ("\"1.23\"^^xsd:numeric", "\"1\"^^xsd:int", true),
            ("\"1.23\"^^xsd:numeric", "\"1\"^^xsd:integer", true),
            ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:decimal", true),
            ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:float", true),
            ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:double", true),
            ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:numeric", true)
          )

          val numericWrongCases = List(
            ("\"1.23\"^^xsd:numeric", "1.24", false),
            ("\"1.23\"^^xsd:numeric", "\"2\"^^xsd:int", false),
            ("\"1.23\"^^xsd:numeric", "\"2\"^^xsd:integer", false),
            ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:decimal", false),
            ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:float", false),
            ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:double", false),
            ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:numeric", false)
          )

          val df = (numericCorrectCases ++ numericWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncForms.gte(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }
      }

      "work for integer values" in {

        val df = List(
          (2, 1),
          (2, 2)
        ).toDF("a", "b")

        df.select(FuncForms.gte(df("a"), df("b"))).collect() shouldEqual Array(
          Row(true),
          Row(true)
        )
      }

      "work in datetimes without a zone" in {

        forAll { datetime: LocalDateTime =>
          val df = List(
            (
              toRDFDateTime(datetime.plusSeconds(1)),
              toRDFDateTime(datetime)
            )
          ).toDF("a", "b")

          df.select(FuncForms.gte(df("a"), df("b")))
            .collect() shouldEqual Array(
            Row(true)
          )
        }
      }

      "work in datetimes with zone" in {

        forAll { datetime: LocalDateTime =>
          val df = List(
            (
              toRDFDateTime(OffsetDateTime.of(datetime, ZoneOffset.ofHours(4))),
              toRDFDateTime(OffsetDateTime.of(datetime, ZoneOffset.ofHours(5)))
            )
          ).toDF("a", "b")

          df.select(
            FuncForms.gte(df("a"), df("b"))
          ).collect() shouldEqual Array(
            Row(true)
          )
        }
      }
    }

    "FuncForms.lte" should {

      "operates on numbers correctly" when {

        "first argument is not typed number and second is other number type" in {
          val plainNumberCorrectCases = List(
            ("1", "2", true),
            ("1", "\"2\"^^xsd:int", true),
            ("1", "\"2\"^^xsd:integer", true),
            ("1", "\"2\"^^xsd:decimal", true),
            ("1.23", "\"1.24\"^^xsd:float", true),
            ("1.23", "\"1.24\"^^xsd:double", true),
            ("1.23", "\"1.24\"^^xsd:numeric", true)
          )

          val plainNumberWrongCases = List(
            ("2", "1", false),
            ("2", "\"1\"^^xsd:int", false),
            ("2", "\"1\"^^xsd:integer", false),
            ("2", "\"1\"^^xsd:decimal", false),
            ("1.24", "\"1.23\"^^xsd:float", false),
            ("1.24", "\"1.23\"^^xsd:double", false),
            ("1.24", "\"1.23\"^^xsd:numeric", false)
          )

          val df = (plainNumberCorrectCases ++ plainNumberWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncForms.lte(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INT type number and second is other number type" in {

          val intCorrectCases = List(
            ("\"1\"^^xsd:int", "2", true),
            ("\"1\"^^xsd:int", "1.1", true),
            ("\"1\"^^xsd:int", "\"2\"^^xsd:int", true),
            ("\"1\"^^xsd:int", "\"2\"^^xsd:integer", true),
            ("\"1\"^^xsd:int", "\"2\"^^xsd:decimal", true),
            ("\"1\"^^xsd:int", "\"1.1\"^^xsd:float", true),
            ("\"1\"^^xsd:int", "\"1.1\"^^xsd:double", true),
            ("\"1\"^^xsd:int", "\"1.1\"^^xsd:numeric", true)
          )

          val intWrongCases = List(
            ("\"2\"^^xsd:int", "1", false),
            ("\"2\"^^xsd:int", "1.1", false),
            ("\"2\"^^xsd:int", "\"1\"^^xsd:int", false),
            ("\"2\"^^xsd:int", "\"1\"^^xsd:integer", false),
            ("\"2\"^^xsd:int", "\"1\"^^xsd:decimal", false),
            ("\"2\"^^xsd:int", "\"1.1\"^^xsd:float", false),
            ("\"2\"^^xsd:int", "\"1.1\"^^xsd:double", false),
            ("\"2\"^^xsd:int", "\"1.1\"^^xsd:numeric", false)
          )

          val df = (intCorrectCases ++ intWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncForms.lte(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INTEGER type number and second is other number type" in {

          val integerCorrectCases = List(
            ("\"1\"^^xsd:integer", "2", true),
            ("\"1\"^^xsd:integer", "1.1", true),
            ("\"1\"^^xsd:integer", "\"2\"^^xsd:int", true),
            ("\"1\"^^xsd:integer", "\"2\"^^xsd:integer", true),
            ("\"1\"^^xsd:integer", "\"2\"^^xsd:decimal", true),
            ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:float", true),
            ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:double", true),
            ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:numeric", true)
          )

          val integerWrongCases = List(
            ("\"2\"^^xsd:integer", "1", false),
            ("\"2\"^^xsd:integer", "1.1", false),
            ("\"2\"^^xsd:integer", "\"1\"^^xsd:int", false),
            ("\"2\"^^xsd:integer", "\"1\"^^xsd:integer", false),
            ("\"2\"^^xsd:integer", "\"1\"^^xsd:decimal", false),
            ("\"2\"^^xsd:integer", "\"1.1\"^^xsd:float", false),
            ("\"2\"^^xsd:integer", "\"1.1\"^^xsd:double", false),
            ("\"2\"^^xsd:integer", "\"1.1\"^^xsd:numeric", false)
          )

          val df = (integerCorrectCases ++ integerWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(FuncForms.lte(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DECIMAL type number and second is other number type" in {

          val decimalCorrectCases = List(
            ("\"1\"^^xsd:decimal", "2", true),
            ("\"1\"^^xsd:decimal", "1.1", true),
            ("\"1\"^^xsd:decimal", "\"2\"^^xsd:int", true),
            ("\"1\"^^xsd:decimal", "\"2\"^^xsd:integer", true),
            ("\"1\"^^xsd:decimal", "\"2\"^^xsd:decimal", true),
            ("\"1\"^^xsd:decimal", "\"1.1\"^^xsd:float", true),
            ("\"1\"^^xsd:decimal", "\"1.1\"^^xsd:double", true),
            ("\"1\"^^xsd:decimal", "\"1.1\"^^xsd:numeric", true)
          )

          val decimalWrongCases = List(
            ("\"2\"^^xsd:decimal", "1", false),
            ("\"2\"^^xsd:decimal", "1.1", false),
            ("\"2\"^^xsd:decimal", "\"1\"^^xsd:int", false),
            ("\"2\"^^xsd:decimal", "\"1\"^^xsd:integer", false),
            ("\"2\"^^xsd:decimal", "\"1\"^^xsd:decimal", false),
            ("\"2\"^^xsd:decimal", "\"1.24\"^^xsd:float", false),
            ("\"2\"^^xsd:decimal", "\"1.24\"^^xsd:double", false),
            ("\"2\"^^xsd:decimal", "\"1.24\"^^xsd:numeric", false)
          )

          val df = (decimalCorrectCases ++ decimalWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncForms.lte(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is FLOAT type number and second is other number type" in {

          val floatCorrectCases = List(
            ("\"1.23\"^^xsd:float", "1.24", true),
            ("\"1.1\"^^xsd:float", "\"2\"^^xsd:int", true),
            ("\"1.1\"^^xsd:float", "\"2\"^^xsd:integer", true),
            ("\"1.1\"^^xsd:float", "\"2\"^^xsd:decimal", true),
            ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:float", true),
            ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:double", true),
            ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:numeric", true)
          )

          val floatWrongCases = List(
            ("\"1.24\"^^xsd:float", "1.23", false),
            ("\"1.24\"^^xsd:float", "\"1\"^^xsd:int", false),
            ("\"1.24\"^^xsd:float", "\"1\"^^xsd:integer", false),
            ("\"1.24\"^^xsd:float", "\"1\"^^xsd:decimal", false),
            ("\"1.24\"^^xsd:float", "\"1.23\"^^xsd:float", false),
            ("\"1.24\"^^xsd:float", "\"1.23\"^^xsd:double", false),
            ("\"1.24\"^^xsd:float", "\"1.23\"^^xsd:numeric", false)
          )

          val df = (floatCorrectCases ++ floatWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncForms.lte(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DOUBLE type number and second is other number type" in {

          val doubleCorrectCases = List(
            ("\"1.23\"^^xsd:double", "1.24", true),
            ("\"1.1\"^^xsd:double", "\"2\"^^xsd:int", true),
            ("\"1.1\"^^xsd:double", "\"2\"^^xsd:integer", true),
            ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:decimal", true),
            ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:float", true),
            ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:double", true),
            ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:numeric", true)
          )

          val doubleWrongCases = List(
            ("\"1.23\"^^xsd:double", "1.22", false),
            ("\"1.23\"^^xsd:double", "\"1\"^^xsd:int", false),
            ("\"1.23\"^^xsd:double", "\"1\"^^xsd:integer", false),
            ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:decimal", false),
            ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:float", false),
            ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:double", false),
            ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:numeric", false)
          )

          val df = (doubleCorrectCases ++ doubleWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncForms.lte(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is NUMERIC type number and second is other number type" in {

          val numericCorrectCases = List(
            ("\"1.23\"^^xsd:numeric", "1.24", true),
            ("\"1.23\"^^xsd:numeric", "\"2\"^^xsd:int", true),
            ("\"1.23\"^^xsd:numeric", "\"2\"^^xsd:integer", true),
            ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:decimal", true),
            ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:float", true),
            ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:double", true),
            ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:numeric", true)
          )

          val numericWrongCases = List(
            ("\"1.23\"^^xsd:numeric", "1.22", false),
            ("\"1.23\"^^xsd:numeric", "\"1\"^^xsd:int", false),
            ("\"1.23\"^^xsd:numeric", "\"1\"^^xsd:integer", false),
            ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:decimal", false),
            ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:float", false),
            ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:double", false),
            ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:numeric", false)
          )

          val df = (numericCorrectCases ++ numericWrongCases).toDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(FuncForms.lte(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }
      }

      "work for integer values" in {

        val df = List(
          (1, 2),
          (2, 2)
        ).toDF("a", "b")

        df.select(FuncForms.lte(df("a"), df("b"))).collect() shouldEqual Array(
          Row(true),
          Row(true)
        )
      }

      "work in datetimes without a zone" in {

        forAll { datetime: LocalDateTime =>
          val df = List(
            (
              toRDFDateTime(datetime),
              toRDFDateTime(datetime.plusSeconds(1))
            )
          ).toDF("a", "b")

          df.select(FuncForms.lte(df("a"), df("b")))
            .collect() shouldEqual Array(
            Row(true)
          )
        }
      }

      "work in datetimes with zone" in {

        forAll { datetime: LocalDateTime =>
          val df = List(
            (
              toRDFDateTime(OffsetDateTime.of(datetime, ZoneOffset.ofHours(5))),
              toRDFDateTime(OffsetDateTime.of(datetime, ZoneOffset.ofHours(4)))
            )
          ).toDF("a", "b")

          df.select(
            FuncForms.lte(df("a"), df("b"))
          ).collect() shouldEqual Array(
            Row(true)
          )
        }
      }
    }

    "FuncForms.and" should {
      // TODO: Implement tests on and
    }

    "FuncForms.or" should {
      // TODO: Implement tests on or
    }

    "FuncForms.negate" should {

      "return the input boolean column negated" in {

        val df = List(
          true,
          false
        ).toDF("boolean")

        val result = df.select(FuncForms.negate(df("boolean"))).collect

        result shouldEqual Array(
          Row(false),
          Row(true)
        )
      }

      "fail when the input column contain values that are not boolean values" in {

        val df = List(
          "a",
          null
        ).toDF("boolean")

        val caught = intercept[AnalysisException] {
          df.select(FuncForms.negate(df("boolean"))).collect
        }

        caught.getMessage should contain
        "cannot resolve '(NOT `boolean`)' due to data type mismatch"
      }
    }

    "FuncForms.in" should {

      "return true when exists" in {

        val e = lit(2)
        val df = List(
          (1, 2, 3)
        ).toDF("e1", "e2", "e3")

        val result =
          df.select(FuncForms.in(e, List(df("e1"), df("e2"), df("e3")))).collect

        result shouldEqual Array(
          Row(true)
        )
      }

      "return false when empty" in {

        val e = lit(2)
        val df = List(
          ""
        ).toDF("e1")

        val result = df.select(FuncForms.in(e, List.empty[Column])).collect

        result shouldEqual Array(
          Row(false)
        )
      }

      "return true when exists with mixed types" in {

        val e = lit(2)
        val df = List(
          ("<http://example/iri>", "str", 2.0)
        ).toDF("e1", "e2", "e3")

        val result =
          df.select(FuncForms.in(e, List(df("e1"), df("e2"), df("e3")))).collect

        result shouldEqual Array(
          Row(true)
        )
      }
    }
  }

  def toRDFDateTime(datetime: TemporalAccessor): String =
    "\"" + DateTimeFormatter
      .ofPattern("yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]")
      .format(datetime) + "\"^^xsd:dateTime"
}
