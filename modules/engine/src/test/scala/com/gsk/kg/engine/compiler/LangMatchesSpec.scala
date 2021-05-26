package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class LangMatchesSpec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
    with TestConfig {

  import sqlContext.implicits._

  "perform langMatches function correctly" when {

    "used with range" in {
      val df = List(
        (
          "<http://uri.com/subject/a1>",
          "<http://xmlns.com/foaf/0.1/title>",
          "\"That Seventies Show\"@en"
        ),
        (
          "<http://uri.com/subject/a1>",
          "<http://xmlns.com/foaf/0.1/title>",
          "\"Cette Série des Années Soixante-dix\"@fr"
        ),
        (
          "<http://uri.com/subject/a1>",
          "<http://xmlns.com/foaf/0.1/title>",
          "\"Cette Série des Années Septante\"@fr-BE"
        ),
        (
          "<http://uri.com/subject/a1>",
          "<http://xmlns.com/foaf/0.1/title>",
          "Il Buono, il Bruto, il Cattivo"
        )
      ).toDF("s", "p", "o")

      val query =
        """
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          SELECT ?title
           WHERE { ?x foaf:title ?title
                   FILTER langMatches(LANG(?title), "FR") }
          """

      val result = Compiler.compile(df, query, config)

      result.right.get.collect.toSet shouldEqual Set(
        Row("\"Cette Série des Années Soixante-dix\"@fr"),
        Row("\"Cette Série des Années Septante\"@fr-BE")
      )
    }

    "used with wildcard" in {
      val df = List(
        (
          "<http://uri.com/subject/a1>",
          "<http://xmlns.com/foaf/0.1/title>",
          "\"That Seventies Show\"@en"
        ),
        (
          "<http://uri.com/subject/a1>",
          "<http://xmlns.com/foaf/0.1/title>",
          "\"Cette Série des Années Soixante-dix\"@fr"
        ),
        (
          "<http://uri.com/subject/a1>",
          "<http://xmlns.com/foaf/0.1/title>",
          "\"Cette Série des Années Septante\"@fr-BE"
        ),
        (
          "<http://uri.com/subject/a1>",
          "<http://xmlns.com/foaf/0.1/title>",
          "Il Buono, il Bruto, il Cattivo"
        )
      ).toDF("s", "p", "o")

      val query =
        """
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          SELECT ?title
           WHERE { ?x foaf:title ?title
                   FILTER langMatches(LANG(?title), "*") }
          """

      val result = Compiler.compile(df, query, config)

      result.right.get.collect.toSet shouldEqual Set(
        Row("\"That Seventies Show\"@en"),
        Row("\"Cette Série des Années Soixante-dix\"@fr"),
        Row("\"Cette Série des Années Septante\"@fr-BE")
      )
    }
  }
}
