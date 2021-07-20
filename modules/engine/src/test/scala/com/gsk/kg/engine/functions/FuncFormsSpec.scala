package com.gsk.kg.engine.functions

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit

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

      "operates on string correctly" in {

        val df = List(
          ("alice", "alice", true),
          ("alice", "\"alice\"", true),
          ("alice", "\"alice\"^^xsd:string", true),
          ("\"alice\"", "\"alice\"", true),
          ("\"alice\"", "\"alice\"^^xsd:string", true),
          ("\"alice\"^^xsd:string", "\"alice\"^^xsd:string", true)
        ).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result   = df.select(FuncForms.equals(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

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

      "return true when exists and there are null expressions" in {

        val e = lit(2)
        val df = List(
          (null, 2)
        ).toDF("e1", "e2")

        val result =
          df.select(FuncForms.in(e, List(df("e1"), df("e2")))).collect

        result shouldEqual Array(
          Row(true)
        )
      }

      "return true when exists and there are null expressions 2" in {

        val e = lit(2)
        val df = List(
          (2, null)
        ).toDF("e1", "e2")

        val result =
          df.select(FuncForms.in(e, List(df("e1"), df("e2")))).collect

        result shouldEqual Array(
          Row(true)
        )
      }

      "return false when not exists and there are null expressions" in {

        val e = lit(2)
        val df = List(
          (3, null)
        ).toDF("e1", "e2")

        val result =
          df.select(FuncForms.in(e, List(df("e1"), df("e2")))).collect

        result shouldEqual Array(
          Row(null)
        )
      }
    }

    "FuncForms.sameTerm" should {

      "return expected results" in {

        val df = List(
          ("\"hello\"", "\"hello\"", true),
          ("\"hello\"@en", "\"hello\"@en", true),
          ("\"hello\"@en", "hello", false),
          ("\"hello\"@en", "\"hello\"@es", false),
          ("\"hello\"@en", "\"hi\"@en", false),
          ("\"1\"^^xsd:int", "\"1\"^^xsd:int", true),
          ("\"1\"^^xsd:int", "1", false),
          ("\"1\"^^xsd:integer", "1", true),
          ("\"1\"^^xsd:int", "\"1\"^^xsd:integer", false),
          ("\"1\"^^xsd:string", "\"1\"^^xsd:integer", false),
          ("\"1\"^^xsd:int", "\"2\"^^xsd:int", false),
          ("\"1.0\"^^xsd:double", "1.0", false),
          ("\"1.0\"^^xsd:decimal", "1.0", true),
          ("\"1.0\"^^xsd:string", "1.0", false),
          ("\"1.0\"^^xsd:string", "\"1.0\"", true),
          ("\"1.0\"^^xsd:decimal", "\"1.0\"", false),
          ("true", "true", true),
          ("\"true\"^^xsd:boolean", "false", false),
          ("\"true\"^^xsd:boolean", "true", true),
          ("\"true\"^^xsd:boolean", "\"true\"^^xsd:boolean", true),
          ("\"true\"^^xsd:boolean", "\"false\"^^xsd:boolean", false),
          (
            "\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>",
            "true",
            true
          ),
          (
            "\"value\"^^<http://www.w3.org/2001/XMLSchema#string>",
            "\"value\"",
            true
          ),
          (
            "\"2011-01-10T14:45:13.815-05:00\"^^xsd:dateTime",
            "\"2011-01-10T14:45:13.815-05:00\"^^xsd:dateTime",
            true
          ),
          (
            "\"2011-01-10T14:45:13.815-05:00\"^^xsd:dateTime",
            "2011-01-10T14:45:13.815-05:00",
            false
          )
        ).toDF("expr1", "expr2", "expected")

        val result =
          df.select(FuncForms.sameTerm(df("expr1"), df("expr2"))).collect
        val expected = df.select(df("expected")).collect()

        result shouldEqual expected
      }
    }

    "FuncForms.if" should {

      "return expected results with plain booleans" in {

        val df = List(
          (true, "yes", "no", "yes"),
          (false, "yes", "no", "no")
        ).toDF("cnd", "ifTrue", "ifFalse", "expected")

        val result =
          df.select(FuncForms.`if`(df("cnd"), df("ifTrue"), df("ifFalse")))
            .collect
        val expected = df.select(df("expected")).collect()

        result shouldEqual expected
      }

      "return if true when other types evaluate to true" in {

        val df = List(
          ("\"true\"^^xsd:boolean", "yes", "no", "yes"),
          ("\"1\"^^xsd:integer", "yes", "no", "yes"),
          ("true", "yes", "no", "yes"),
          ("\"abc\"^^xsd:string", "yes", "no", "yes"),
          ("abc", "yes", "no", "yes"),
          ("\"1.2\"^^xsd:double", "yes", "no", "yes"),
          ("1.2", "yes", "no", "yes")
        ).toDF("cnd", "ifTrue", "ifFalse", "expected")

        val result =
          df.select(FuncForms.`if`(df("cnd"), df("ifTrue"), df("ifFalse")))
            .collect
        val expected = df.select(df("expected")).collect()

        result shouldEqual expected
      }

      "return if false when other types evaluate to false" in {

        val df = List(
          ("\"abc\"^^xsd:boolean", "yes", "no", "no"),
          ("\"false\"^^xsd:boolean", "yes", "no", "no"),
          ("\"abc\"^^xsd:integer", "yes", "no", "no"),
          ("false", "yes", "no", "no"),
          ("\"\"^^xsd:string", "yes", "no", "no"),
          ("", "yes", "no", "no"),
          ("\"0\"^^xsd:integer", "yes", "no", "no"),
          ("0", "yes", "no", "no"),
          ("NaN", "yes", "no", "no"),
          ("\"NaN\"^^xsd:double", "yes", "no", "no")
        ).toDF("cnd", "ifTrue", "ifFalse", "expected")

        val result =
          df.select(FuncForms.`if`(df("cnd"), df("ifTrue"), df("ifFalse")))
            .collect
        val expected = df.select(df("expected")).collect()

        result shouldEqual expected
      }

      "return null when other types evaluate to null" in {

        val df = List(
          (null, "yes", "no", null)
        ).toDF("cnd", "ifTrue", "ifFalse", "expected")

        val result =
          df.select(FuncForms.`if`(df("cnd"), df("ifTrue"), df("ifFalse")))
            .collect
        val expected = df.select(df("expected")).collect()

        result shouldEqual expected
      }
    }

    "FuncForms.effectiveBooleanValue" should {

      "return expected values" in {
        val df = List(
          ("\"abc\"^^xsd:boolean", false),
          ("\"false\"^^xsd:boolean", false),
          ("\"true\"^^xsd:boolean", true),
          ("\"abc\"^^xsd:integer", false),
          ("\"1\"^^xsd:integer", true),
          ("true", true),
          ("false", false),
          ("\"\"^^xsd:string", false),
          ("", false),
          ("\"abc\"^^xsd:string", true),
          ("abc", true),
          ("\"0\"^^xsd:integer", false),
          ("0", false),
          ("NaN", false),
          ("\"NaN\"^^xsd:double", false),
          ("\"1.2\"^^xsd:double", true),
          ("1.2", true)
        ).toDF("elem", "expected")

        val result =
          df.select(FuncForms.effectiveBooleanValue(df("elem"))).collect()
        val expected = df.select(df("expected")).collect()

        result shouldEqual expected
      }
    }

    "FuncForms.bound" should {

      "return expected value" in {

        val df = List(
          ("\"abc\"^^xsd:boolean", true),
          ("NaN", true),
          ("INF", true),
          (null, false)
        ).toDF("elem", "expected")

        val result =
          df.select(FuncForms.bound(df("elem"))).collect()
        val expected = df.select(df("expected")).collect()

        result shouldEqual expected
      }
    }

    "FuncForms.coalesce" should {

      "return expected value" when {

        "first elem bound and second null" in {
          val df = List(
            ("2", null, "2")
          ).toDF("?x", "?y", "expected")

          val cols = List(df("?x"), lit(null))

          val result   = df.select(FuncForms.coalesce(cols)).collect()
          val expected = df.select(df("expected")).collect()

          result shouldEqual expected
        }

        "first elem null and second bound" in {
          val df = List(
            ("2", null, "2")
          ).toDF("?x", "?y", "expected")

          val cols = List(lit(null), df("?x"))

          val result   = df.select(FuncForms.coalesce(cols)).collect()
          val expected = df.select(df("expected")).collect()

          result shouldEqual expected
        }

        "first elem literal and second bound" in {
          val df = List(
            ("2", null, "5")
          ).toDF("?x", "?y", "expected")

          val cols = List(lit("5"), df("?x"))

          val result   = df.select(FuncForms.coalesce(cols)).collect()
          val expected = df.select(df("expected")).collect()

          result shouldEqual expected
        }

        "first elem not bound and second literal" in {
          val df = List(
            ("2", null, "3")
          ).toDF("?x", "?y", "expected")

          val cols = List(df("?y"), lit("3"))

          val result   = df.select(FuncForms.coalesce(cols)).collect()
          val expected = df.select(df("expected")).collect()

          result shouldEqual expected
        }

        "one elem not bound" in {
          val df = List(
            ("2", null, null)
          ).toDF("?x", "?y", "expected")

          val cols = List(df("?y"))

          val result   = df.select(FuncForms.coalesce(cols)).collect()
          val expected = df.select(df("expected")).collect()

          result shouldEqual expected
        }
      }
    }
  }

  def toRDFDateTime(datetime: TemporalAccessor): String =
    "\"" + DateTimeFormatter
      .ofPattern("yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]")
      .format(datetime) + "\"^^xsd:dateTime"
}
