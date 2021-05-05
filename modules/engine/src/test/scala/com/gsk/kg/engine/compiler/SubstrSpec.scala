package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SubstrSpec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
    with TestConfig {

  import sqlContext.implicits._

  "perform SUBSTR function correctly" when {

    "used on string literals with a specified length" in {
      val df = List(
        (
          "http://uri.com/subject/#a1",
          "http://xmlns.com/foaf/0.1/name",
          "Alice"
        ),
        (
          "http://uri.com/subject/#a3",
          "http://xmlns.com/foaf/0.1/name",
          "Alison"
        )
      ).toDF("s", "p", "o")

      val query =
        """
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          CONSTRUCT {
            ?x foaf:subName ?subName .
          }
          WHERE{
            ?x foaf:name ?name .
            BIND(SUBSTR(?name, 1, 1) as ?subName) .
          }
          """

      val result = Compiler.compile(df, query, config)

      result.right.get.drop("s", "p").collect.toSet shouldEqual Set(
        Row("\"A\""),
        Row("\"A\"")
      )
    }

    "used on string literals without a specified length" in {
      val df = List(
        (
          "http://uri.com/subject/#a1",
          "http://xmlns.com/foaf/0.1/name",
          "Alice"
        ),
        (
          "http://uri.com/subject/#a3",
          "http://xmlns.com/foaf/0.1/name",
          "Alison"
        )
      ).toDF("s", "p", "o")

      val query =
        """
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          CONSTRUCT {
            ?x foaf:subName ?subName .
          }
          WHERE{
            ?x foaf:name ?name .
            BIND(SUBSTR(?name, 3) as ?subName) .
          }
          """

      val result = Compiler.compile(df, query, config)

      result.right.get.drop("s", "p").collect.toSet shouldEqual Set(
        Row("\"ice\""),
        Row("\"ison\"")
      )
    }
  }
}
