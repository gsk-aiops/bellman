package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class IfSpec extends AnyWordSpec with Matchers with SparkSpec with TestConfig {

  import sqlContext.implicits._

  "perform query with IF function" when {

    "simple IF" in {

      val df = List(
        (
          "<http://example.org/alice>",
          "<http://xmlns.com/foaf/0.1/name>",
          "Alice"
        ),
        (
          "<http://example.org/alice>",
          "<http://xmlns.com/foaf/0.1/age>",
          "21"
        ),
        (
          "<http://example.org/bob>",
          "<http://xmlns.com/foaf/0.1/name>",
          "Bob"
        ),
        (
          "<http://example.org/bob>",
          "<http://xmlns.com/foaf/0.1/age>",
          "15"
        )
      ).toDF("s", "p", "o")

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT ?name ?cat
          |WHERE {
          | ?s foaf:name ?name .
          | ?s foaf:age ?age .
          | BIND(IF(?age < 18, "child", "adult") AS ?cat)
          |}
          |""".stripMargin

      val result = Compiler.compile(df, query, config).right.get

      result.collect.toSet shouldEqual Set(
        Row("\"Alice\"", "\"adult\""),
        Row("\"Bob\"", "\"child\"")
      )
    }
  }
}
