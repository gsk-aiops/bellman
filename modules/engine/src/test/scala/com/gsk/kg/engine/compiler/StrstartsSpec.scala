package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class StrstartsSpec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
    with TestConfig {

  import sqlContext.implicits._

  "perform STRSTARTS function correctly" when {

    "used on string literals" in {
      val df = List(
        (
          "<http://uri.com/subject/a1>",
          "<http://xmlns.com/foaf/0.1/name>",
          "Alice"
        ),
        (
          "<http://uri.com/subject/a2>",
          "<http://xmlns.com/foaf/0.1/name>",
          "alice"
        ),
        (
          "<http://uri.com/subject/a5>",
          "<http://xmlns.com/foaf/0.1/name>",
          "Alex"
        ),
        (
          "<http://uri.com/subject/a6>",
          "<http://xmlns.com/foaf/0.1/name>",
          "alex"
        ),
        (
          "<http://uri.com/subject/a7>",
          "<http://xmlns.com/foaf/0.1/name>",
          "Adam"
        ),
        (
          "<http://uri.com/subject/a8>",
          "<http://xmlns.com/foaf/0.1/name>",
          "adam"
        )
      ).toDF("s", "p", "o")

      val query =
        """
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          SELECT ?name
           WHERE { ?x foaf:name  ?name
                   FILTER strstarts(?name, "al") }
          """

      val result = Compiler.compile(df, query, config)

      result.right.get.collect.toSet shouldEqual Set(
        Row("\"alice\""),
        Row("\"alex\"")
      )
    }
  }
}
