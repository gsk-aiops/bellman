package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class EqualsSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "perform query with EQUALS filter" when {

    "equals is plain string" in {

      val df = List(
        (
          "<http://example.org/a>",
          "<http://xmlns.org/foaf/0.1/name>",
          "\"Alice\""
        ),
        (
          "<http://example.org/b>",
          "<http://xmlns.org/foaf/0.1/name>",
          "Alice"
        ),
        (
          "<http://example.org/c>",
          "<http://xmlns.org/foaf/0.1/name>",
          "\"Alice\"^^xsd:string"
        ),
        (
          "<http://example.org/d>",
          "<http://xmlns.org/foaf/0.1/name>",
          "\"\"Alice\"^^xsd:string\""
        )
      ).toDF("s", "p", "o")

      val query =
        """
          |SELECT ?s ?o
          |WHERE {
          | ?s ?p ?o .
          | FILTER (?o = "Alice")
          |}
          |""".stripMargin

      val result = Compiler.compile(df, query, config)

      result.right.get.collect.toSet shouldEqual Set(
        Row("<http://example.org/a>", "\"Alice\""),
        Row("<http://example.org/b>", "\"Alice\""),
        Row("<http://example.org/c>", "\"Alice\"^^xsd:string"),
        Row("<http://example.org/d>", "\"\"Alice\"^^xsd:string\"")
      )
    }

    "equals typed string" in {

      val df = List(
        (
          "<http://example.org/a>",
          "<http://xmlns.org/foaf/0.1/name>",
          "\"Alice\""
        ),
        (
          "<http://example.org/b>",
          "<http://xmlns.org/foaf/0.1/name>",
          "Alice"
        ),
        (
          "<http://example.org/c>",
          "<http://xmlns.org/foaf/0.1/name>",
          "\"Alice\"^^xsd:string"
        ),
        (
          "<http://example.org/d>",
          "<http://xmlns.org/foaf/0.1/name>",
          "\"\"Alice\"^^xsd:string\""
        )
      ).toDF("s", "p", "o")

      val query =
        """
          |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
          |
          |SELECT ?s ?o
          |WHERE {
          | ?s ?p ?o .
          | FILTER (?o = "Alice"^^xsd:string)
          |}
          |""".stripMargin

      val result = Compiler.compile(df, query, config)

      result.right.get.collect.toSet shouldEqual Set(
        Row("<http://example.org/a>", "\"Alice\""),
        Row("<http://example.org/b>", "\"Alice\""),
        Row("<http://example.org/c>", "\"Alice\"^^xsd:string"),
        Row("<http://example.org/d>", "\"\"Alice\"^^xsd:string\"")
      )
    }
  }
}
