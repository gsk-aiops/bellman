package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class OptionalSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "perform query with OPTIONAL" should {

    "execute and obtain expected results with simple optional" in {

      val df: DataFrame = List(
        (
          "_:a",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://xmlns.com/foaf/0.1/Person>",
          ""
        ),
        ("_:a", "<http://xmlns.com/foaf/0.1/name>", "Alice", ""),
        (
          "_:a",
          "<http://xmlns.com/foaf/0.1/mbox>",
          "<mailto:alice@example.com>",
          ""
        ),
        (
          "_:a",
          "<http://xmlns.com/foaf/0.1/mbox>",
          "<mailto:alice@work.example>",
          ""
        ),
        (
          "_:b",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://xmlns.com/foaf/0.1/Person>",
          ""
        ),
        ("_:b", "<http://xmlns.com/foaf/0.1/name>", "Bob", "")
      ).toDF("s", "p", "o", "g")

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |SELECT ?name ?mbox
          |WHERE  { ?x foaf:name  ?name .
          |         OPTIONAL { ?x  foaf:mbox  ?mbox }
          |       }
          |""".stripMargin

      val result = Compiler.compile(df, query, config)

      result shouldBe a[Right[_, _]]
      result.right.get.collect.length shouldEqual 3
      result.right.get.collect.toSet shouldEqual Set(
        Row("\"Alice\"", "<mailto:alice@example.com>"),
        Row("\"Alice\"", "<mailto:alice@work.example>"),
        Row("\"Bob\"", null)
      )
    }

    "execute and obtain expected results with constraints in optional" in {

      val df: DataFrame = List(
        (
          "_:book1",
          "<http://purl.org/dc/elements/1.1/title>",
          "SPARQL Tutorial",
          ""
        ),
        ("_:book1", "<http://example.org/ns#price>", "42", ""),
        (
          "_:book2",
          "<http://purl.org/dc/elements/1.1/title>",
          "The Semantic Web",
          ""
        ),
        ("_:book2", "<http://example.org/ns#price>", "_:23", "")
      ).toDF("s", "p", "o", "g")

      val query =
        """
          |PREFIX  dc:  <http://purl.org/dc/elements/1.1/>
          |PREFIX  ns:  <http://example.org/ns#>
          |
          |SELECT  ?title ?price
          |WHERE   {
          |   ?x dc:title ?title .
          |   OPTIONAL {
          |     ?x ns:price ?price
          |     FILTER(isBlank(?price))
          |   }
          |}
          |""".stripMargin

      val result = Compiler.compile(df, query, config)

      result shouldBe a[Right[_, _]]
      result.right.get.collect.length shouldEqual 2
      result.right.get.collect.toSet shouldEqual Set(
        Row("\"SPARQL Tutorial\"", null),
        Row("\"The Semantic Web\"", "_:23")
      )
    }

    "execute and obtain expected results with multiple optionals" in {

      val df: DataFrame = List(
        ("_:a", "<http://xmlns.com/foaf/0.1/name>", "Alice", ""),
        (
          "_:a",
          "<http://xmlns.com/foaf/0.1/homepage>",
          "<http://work.example.org/alice/>",
          ""
        ),
        ("_:b", "<http://xmlns.com/foaf/0.1/name>", "Bob", ""),
        (
          "_:b",
          "<http://xmlns.com/foaf/0.1/mbox>",
          "<mailto:bob@work.example>",
          ""
        )
      ).toDF("s", "p", "o", "g")

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT ?name ?mbox ?hpage
          |WHERE  {
          |   ?x foaf:name  ?name .
          |   OPTIONAL { ?x foaf:mbox ?mbox } .
          |   OPTIONAL { ?x foaf:homepage ?hpage }
          |}
          |""".stripMargin

      val result = Compiler.compile(df, query, config)

      result shouldBe a[Right[_, _]]
      result.right.get.collect.length shouldEqual 2
      result.right.get.collect.toSet shouldEqual Set(
        Row("\"Alice\"", null, "<http://work.example.org/alice/>"),
        Row("\"Bob\"", "<mailto:bob@work.example>", null)
      )
    }
  }
}
