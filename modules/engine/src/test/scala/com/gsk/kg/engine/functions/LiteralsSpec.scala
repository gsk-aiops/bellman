package com.gsk.kg.engine.functions

import org.apache.spark.sql.Row

import com.gsk.kg.engine.functions.Literals.DateLiteral
import com.gsk.kg.engine.functions.Literals.TypedLiteral
import com.gsk.kg.engine.scalacheck.CommonGenerators

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class LiteralsSpec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
    with ScalaCheckDrivenPropertyChecks
    with CommonGenerators {

  import sqlContext.implicits._

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  "Literals" when {

    "DateLiteral" when {

      "parseDateFromRDFDateTime" should {

        "work for all types of dates specified by RDF spec" in {

          val df = List(
            """"2001-10-26T21:32:52"^^xsd:dateTime""",
            """"2001-10-26T21:32:52+02:00"^^xsd:dateTime""",
            """"2001-10-26T19:32:52Z"^^xsd:dateTime""",
            """"2001-10-26T19:32:52+00:00"^^xsd:dateTime""",
            """"2001-10-26T21:32:52.12679"^^xsd:dateTime"""
          ).toDF("date")

          df.select(DateLiteral.parseDateFromRDFDateTime(df("date")))
            .collect()
            .map(_.get(0)) shouldNot contain(null)
        }
      }
    }

    "TypedLiteral" when {

      "isTypedLiteral" should {

        "identify RDF literals correctly" in {

          val initial = List(
            ("\"1\"^^xsd:int", true),
            ("\"1.1\"^^xsd:decimal", true),
            ("\"1.1\"^^xsd:float", true),
            ("\"1.1\"^^xsd:double", true),
            ("\"1\"", false),
            ("1", false),
            ("false", false)
          ).toDF("input", "expected")

          val df =
            initial.withColumn(
              "result",
              TypedLiteral.isTypedLiteral(initial("input"))
            )

          df.collect.foreach { case Row(_, expected, result) =>
            expected shouldEqual result
          }
        }
      }
    }
  }

}
