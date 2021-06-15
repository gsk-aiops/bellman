package com.gsk.kg.engine.functions

import org.apache.spark.sql.Row

import com.gsk.kg.engine.compiler.SparkSpec
import com.gsk.kg.engine.scalacheck.CommonGenerators

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import org.apache.spark.sql.functions.col

class FuncTermsSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with ScalaCheckDrivenPropertyChecks
    with CommonGenerators {

  import sqlContext.implicits._

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  "Funcs on RDF Terms" when {

    "FuncTerms.str" should {

      "remove angle brackets from uris" in {

        val initial = List(
          ("<mailto:pepe@examplle.com>", "mailto:pepe@examplle.com"),
          ("<http://example.com>", "http://example.com")
        ).toDF("input", "expected")

        val df = initial.withColumn("result", FuncTerms.str(initial("input")))

        df.collect.foreach { case Row(_, expected, result) =>
          expected shouldEqual result
        }
      }

      "don't modify non-uri strings" in {

        val initial = List(
          ("mailto:pepe@examplle.com>", "mailto:pepe@examplle.com>"),
          ("http://example.com>", "http://example.com>"),
          ("hello", "hello"),
          ("\"test\"", "\"test\""),
          ("1", "1")
        ).toDF("input", "expected")

        val df = initial.withColumn("result", FuncTerms.str(initial("input")))

        df.collect.foreach { case Row(_, expected, result) =>
          expected shouldEqual result
        }
      }
    }

    "FuncTerms.strdt" should {

      "return a literal with lexical for and type specified" in {

        val df = List(
          "123"
        ).toDF("s")

        val result = df
          .select(
            FuncTerms
              .strdt(df("s"), "<http://www.w3.org/2001/XMLSchema#integer>")
          )
          .collect

        result shouldEqual Array(
          Row("\"123\"^^<http://www.w3.org/2001/XMLSchema#integer>")
        )
      }
    }

    "FuncTerms.iri" should {

      "do nothing for IRIs" in {

        val df = List(
          "http://google.com",
          "http://other.com"
        ).toDF("text")

        df.select(FuncTerms.iri(df("text")).as("result"))
          .collect shouldEqual Array(
          Row("<http://google.com>"),
          Row("<http://other.com>")
        )
      }
    }

    "FuncTerms.uri" should {
      // TODO: Add tests for uri
    }

    "FuncTerms.lang" should {

      "correctly return language tag" in {
        val initial = List(
          ("\"Los Angeles\"@en", "en"),
          ("\"Los Angeles\"@es", "es"),
          ("\"Los Angeles\"@en-US", "en-US"),
          ("Los Angeles", "")
        ).toDF("input", "expected")

        val df = initial.withColumn("result", FuncTerms.lang(initial("input")))

        df.collect.foreach { case Row(_, expected, result) =>
          expected shouldEqual result
        }
      }
    }

    "FuncTerms.isBlank" should {

      "return whether a node is a blank node or not" in {

        val df = List(
          "_:a",
          "a:a",
          "_:1",
          "1:1",
          "foaf:name",
          "_:name"
        ).toDF("text")

        val result = df.select(FuncTerms.isBlank(df("text"))).collect

        result shouldEqual Array(
          Row(true),
          Row(false),
          Row(true),
          Row(false),
          Row(false),
          Row(true)
        )
      }
    }

    "FuncTerms.isNumeric" should {

      "return true when the term is numeric" in {
        val initial = List(
          ("\"1\"^^xsd:int", true),
          ("\"1.1\"^^xsd:decimal", true),
          ("\"1.1\"^^xsd:float", true),
          ("\"1.1\"^^xsd:double", true),
          ("1", true),
          ("1.111111", true),
          ("-1.111", true),
          ("-1", true),
          ("0.0", true),
          ("0", true)
        ).toDF("input", "expected")

        val df =
          initial.withColumn("result", FuncTerms.isNumeric(initial("input")))

        df.collect.foreach { case Row(_, expected, result) =>
          expected shouldEqual result
        }
      }

      "return false when the term is not numeric" in {
        val initial = List(
          ("\"1200\"^^xsd:byte", false),
          ("\"1.1\"^^xsd:something", false),
          ("asdfsadfasdf", false),
          ("\"1.1\"", false)
        ).toDF("input", "expected")

        val df =
          initial.withColumn("result", FuncTerms.isNumeric(initial("input")))

        df.collect.foreach { case Row(_, expected, result) =>
          expected shouldEqual result
        }
      }
    }

    "FuncTerms.isLiteral" should {
      // TODO: Add tests for isLiteral
    }

    "FuncTerms.uuid" should {

      "return an uuid value from the column" in {

        val elems = List(1, 2, 3)
        val df    = elems.toDF("a")
        val dfResult = df.withColumn("uuid", FuncTerms.uuid())
          .select(
            col("a"),
            col("uuid")
          )

        dfResult.show(false)

        dfResult
          .select(
            col("uuid").rlike("-").as("regexok")
          ).select("regexok").collect().toSet shouldEqual(Set(Row(true)))
      }
    }
  }
}
