package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class StrbeforeSpec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
    with TestConfig {

  import sqlContext.implicits._

  "perform STRBEFORE function correctly" when {

    "used on string literals" in {
      val df = List(
        (
          "<http://uri.com/subject/#a1>",
          "<http://xmlns.com/foaf/0.1/name>",
          "Alice#Person"
        ),
        (
          "<http://uri.com/subject/#a2>",
          "<http://xmlns.com/foaf/0.1/name>",
          "Alex#Person"
        ),
        (
          "<http://uri.com/subject/#a3>",
          "<http://xmlns.com/foaf/0.1/name>",
          "Alison"
        )
      ).toDF("s", "p", "o")

      val query =
        """
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          CONSTRUCT {
            ?x foaf:firstName ?firstName .
          }
          WHERE{
            ?x foaf:name ?name .
            BIND(STRBEFORE(?name, "#") as ?firstName) .
          }
          """

      val result = Compiler.compile(df, query, config)

      result.right.get.drop("s", "p").collect.toSet shouldEqual Set(
        Row("\"Alice\""),
        Row("\"Alex\""),
        Row("\"\"")
      )
    }
  }
}
