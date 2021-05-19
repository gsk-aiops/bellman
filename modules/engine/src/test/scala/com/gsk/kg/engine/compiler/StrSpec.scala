package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class StrSpec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
    with TestConfig {

  import sqlContext.implicits._

  "perform STR function correctly" when {

    "used on string literals" in {
      val df = List(
        (
          "<http://gsk-kg.rdip.gsk.com/nlp/doc#2020-09-28T11:39:06.610Zdead25dd4360486aa4e0b2901359373c>",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>"
        ),
        (
          "<http://gsk-kg.rdip.gsk.com/nlp/doc#2020-09-28T11:44:54.962Z593fefad8e434e29b1e3c774b112f3d0>",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>"
        )
      ).toDF("s", "p", "o")

      val query =
        """
          PREFIX  dm:   <http://gsk-kg.rdip.gsk.com/dm/1.0/>
          PREFIX  litn:  <http://lit-search-api/node/>
          PREFIX  litp:  <http://lit-search-api/property/>

          CONSTRUCT {
            ?Document a litn:Document .
            ?Document litp:docID ?docid .
          }
          WHERE{
            ?d a dm:Document .
            BIND(STRAFTER(str(?d), "#") as ?docid) .
            BIND(URI(CONCAT("http://lit-search-api/node/doc#", ?docid)) as ?Document) .
          }
          """

      val result = Compiler.compile(df, query, config).right.get

      result.collect.toSet shouldEqual Set(
        Row(
          "<http://lit-search-api/node/doc#2020-09-28T11:39:06.610Zdead25dd4360486aa4e0b2901359373c>",
          "<http://lit-search-api/property/docID>",
          "\"2020-09-28T11:39:06.610Zdead25dd4360486aa4e0b2901359373c\""
        ),
        Row(
          "<http://lit-search-api/node/doc#2020-09-28T11:39:06.610Zdead25dd4360486aa4e0b2901359373c>",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://lit-search-api/node/Document>"
        ),
        Row(
          "<http://lit-search-api/node/doc#2020-09-28T11:44:54.962Z593fefad8e434e29b1e3c774b112f3d0>",
          "<http://lit-search-api/property/docID>",
          "\"2020-09-28T11:44:54.962Z593fefad8e434e29b1e3c774b112f3d0\""
        ),
        Row(
          "<http://lit-search-api/node/doc#2020-09-28T11:44:54.962Z593fefad8e434e29b1e3c774b112f3d0>",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://lit-search-api/node/Document>"
        )
      )
    }

    "implement isliteral correctly" in {
      val df = List(
        (
          "\"a\"",
          "\"b\"",
          "\"c\""
        ),
        (
          "\"a\"",
          "\"b\"",
          "\"c\"^^xsd:string"
        ),
        (
          "\"a\"",
          "\"b\"",
          "\"c\"@fr"
        ),
        (
          "\"a\"",
          "\"b\"",
          "true"
        ),
        (
          "\"a\"",
          "\"b\"",
          "3"
        ),
        (
          "\"a\"",
          "\"b\"",
          "<http://example.com/object>"
        )
      ).toDF("s", "p", "o")

      val query =
        """
          SELECT ISLITERAL(?o)
          WHERE{
            ?s ?p ?o
          }
        """

      val result = Compiler.compile(df, query, config) match {
	        case Left(a) => throw new RuntimeException(a.toString)
	        case Right(b) => b
        }

      result.collect.toSet shouldEqual Set(
        Row("true"),
        Row("true"),
        Row("true"),
        Row("false"),
        Row("false"),
        Row("false")
      )
    }
  }
}
