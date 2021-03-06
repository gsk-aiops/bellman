package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class RegexSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "perform REGEX function correctly" when {

    "used without flags" in {
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
        )
      ).toDF("s", "p", "o")

      val query =
        """
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          SELECT ?name
           WHERE { ?x foaf:name  ?name
                   FILTER regex(?name, "^ali") }
          """

      val result = Compiler.compile(df, query, config)

      result.right.get.collect.toSet shouldEqual Set(
        Row("\"alice\"")
      )
    }

    "used with flags" in {
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
        )
      ).toDF("s", "p", "o")

      val query =
        """
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          SELECT ?name
           WHERE { ?x foaf:name  ?name
                   FILTER regex(?name, "^ali", "i") }
          """

      val result = Compiler.compile(df, query, config)

      result.right.get.collect.toSet shouldEqual Set(
        Row("\"Alice\""),
        Row("\"alice\"")
      )
    }
  }
}
