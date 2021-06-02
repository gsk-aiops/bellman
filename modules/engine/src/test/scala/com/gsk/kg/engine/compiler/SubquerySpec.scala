package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.EngineError.ParsingError
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SubquerySpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "perform SUBQUERY" should {

    "fail to parse" when {

      "inner CONSTRUCT as graph pattern" in {

        val df: DataFrame = List(
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Alice Foo",
            ""
          ),
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/name>",
            "A. Foo",
            ""
          ),
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/knows>",
            "<http://example.org/bob>",
            ""
          ),
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/knows>",
            "<http://example.org/carol>",
            ""
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Bob",
            ""
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Bob Bar",
            ""
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/name>",
            "B. Bar",
            ""
          ),
          (
            "<http://example.org/carol>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Carol",
            ""
          ),
          (
            "<http://example.org/carol>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Carol Baz",
            ""
          ),
          (
            "<http://example.org/carol>",
            "<http://xmlns.com/foaf/0.1/name>",
            "C. Baz",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?y ?name
            |WHERE {
            |  ?y foaf:knows ?x .
            |  {
            |    CONSTRUCT {
            |      ?x foaf:friends ?name .
            |    } WHERE {
            |      ?x foaf:name ?name .
            |    }
            |  }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)
        result shouldBe a[Left[_, _]]
        result.left.get shouldBe a[ParsingError]
      }
    }

    "execute and obtain expected results" when {

      "outer SELECT and inner SELECT as graph pattern" in {

        val df: DataFrame = List(
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Alice Foo",
            ""
          ),
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/name>",
            "A. Foo",
            ""
          ),
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/knows>",
            "<http://example.org/bob>",
            ""
          ),
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/knows>",
            "<http://example.org/carol>",
            ""
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Bob",
            ""
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Bob Bar",
            ""
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/name>",
            "B. Bar",
            ""
          ),
          (
            "<http://example.org/carol>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Carol",
            ""
          ),
          (
            "<http://example.org/carol>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Carol Baz",
            ""
          ),
          (
            "<http://example.org/carol>",
            "<http://xmlns.com/foaf/0.1/name>",
            "C. Baz",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?y ?name
            |WHERE {
            |  ?y foaf:knows ?x .
            |  {
            |    SELECT ?x ?name
            |    WHERE {
            |      ?x foaf:name ?name .
            |    }
            |  }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 6
        result.right.get.collect.toSet shouldEqual Set(
          Row("<http://example.org/alice>", "\"Carol Baz\""),
          Row("<http://example.org/alice>", "\"Bob Bar\""),
          Row("<http://example.org/alice>", "\"Bob\""),
          Row("<http://example.org/alice>", "\"Carol\""),
          Row("<http://example.org/alice>", "\"B. Bar\""),
          Row("<http://example.org/alice>", "\"C. Baz\"")
        )
      }

      // TODO: Un-ignore when implemented ASK
      "outer SELECT and inner ASK as graph pattern" ignore {}

      "outer CONSTRUCT and inner SELECT as graph pattern" in {
        val df: DataFrame = List(
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Alice Foo",
            ""
          ),
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/name>",
            "A. Foo",
            ""
          ),
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/knows>",
            "<http://example.org/bob>",
            ""
          ),
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/knows>",
            "<http://example.org/carol>",
            ""
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Bob",
            ""
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Bob Bar",
            ""
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/name>",
            "B. Bar",
            ""
          ),
          (
            "<http://example.org/carol>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Carol",
            ""
          ),
          (
            "<http://example.org/carol>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Carol Baz",
            ""
          ),
          (
            "<http://example.org/carol>",
            "<http://xmlns.com/foaf/0.1/name>",
            "C. Baz",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |
            |CONSTRUCT {
            |  ?y foaf:knows ?name .
            |} WHERE {
            |  ?y foaf:knows ?x .
            |  {
            |    SELECT ?x ?name
            |    WHERE {
            |      ?x foaf:name ?name .
            |    }
            |  }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 6
        result.right.get.collect.toSet shouldEqual Set(
          Row(
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/knows>",
            "\"Carol Baz\""
          ),
          Row(
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/knows>",
            "\"Bob Bar\""
          ),
          Row(
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/knows>",
            "\"Bob\""
          ),
          Row(
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/knows>",
            "\"Carol\""
          ),
          Row(
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/knows>",
            "\"B. Bar\""
          ),
          Row(
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/knows>",
            "\"C. Baz\""
          )
        )
      }

      // TODO: Un-ignore when implemented ASK
      "outer CONSTRUCT and inner ASK as graph pattern" ignore {}

      // TODO: Un-ignore when implemented ASK
      "outer ASK and inner SELECT as graph pattern" ignore {}

      "multiple inner sub-queries" in {

        val df: DataFrame = List(
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/member>",
            "Family",
            ""
          ),
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Alice Foo",
            ""
          ),
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/name>",
            "A. Foo",
            ""
          ),
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/knows>",
            "<http://example.org/bob>",
            ""
          ),
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/knows>",
            "<http://example.org/carol>",
            ""
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Bob",
            ""
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Bob Bar",
            ""
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/name>",
            "B. Bar",
            ""
          ),
          (
            "<http://example.org/carol>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Carol",
            ""
          ),
          (
            "<http://example.org/carol>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Carol Baz",
            ""
          ),
          (
            "<http://example.org/carol>",
            "<http://xmlns.com/foaf/0.1/name>",
            "C. Baz",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?x ?y ?name ?group
            |WHERE {
            |  ?y foaf:member ?group .
            |  {
            |    SELECT ?y ?x ?name
            |    WHERE {
            |      ?y foaf:knows ?x .
            |      {
            |         SELECT ?x ?name
            |         WHERE {
            |           ?x foaf:name ?name .
            |         }
            |      }
            |    }
            |  }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 6
        result.right.get.collect.toSet shouldEqual Set(
          Row(
            "<http://example.org/bob>",
            "<http://example.org/alice>",
            "\"B. Bar\"",
            "\"Family\""
          ),
          Row(
            "<http://example.org/carol>",
            "<http://example.org/alice>",
            "\"Carol\"",
            "\"Family\""
          ),
          Row(
            "<http://example.org/bob>",
            "<http://example.org/alice>",
            "\"Bob Bar\"",
            "\"Family\""
          ),
          Row(
            "<http://example.org/carol>",
            "<http://example.org/alice>",
            "\"C. Baz\"",
            "\"Family\""
          ),
          Row(
            "<http://example.org/bob>",
            "<http://example.org/alice>",
            "\"Bob\"",
            "\"Family\""
          ),
          Row(
            "<http://example.org/carol>",
            "<http://example.org/alice>",
            "\"Carol Baz\"",
            "\"Family\""
          )
        )
      }

      "mixing graphs" in {

        val df: DataFrame = List(
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/member>",
            "Family",
            ""
          ),
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Alice Foo",
            "<http://some-other.ttl>"
          ),
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/name>",
            "A. Foo",
            "<http://some-other.ttl>"
          ),
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/knows>",
            "<http://example.org/bob>",
            ""
          ),
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/knows>",
            "<http://example.org/carol>",
            ""
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Bob",
            "<http://some-other.ttl>"
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Bob Bar",
            "<http://some-other.ttl>"
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/name>",
            "B. Bar",
            "<http://some-other.ttl>"
          ),
          (
            "<http://example.org/carol>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Carol",
            "<http://some-other.ttl>"
          ),
          (
            "<http://example.org/carol>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Carol Baz",
            "<http://some-other.ttl>"
          ),
          (
            "<http://example.org/carol>",
            "<http://xmlns.com/foaf/0.1/name>",
            "C. Baz",
            "<http://some-other.ttl>"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?x ?name
            |FROM NAMED <http://some-other.ttl>
            |WHERE {
            |  GRAPH <http://some-other.ttl> {
            |    {
            |      SELECT ?x ?name
            |      WHERE {
            |        ?x foaf:name ?name .
            |      }
            |    }
            |  }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 8
        result.right.get.collect.toSet shouldEqual Set(
          Row(
            "<http://example.org/carol>",
            "\"Carol\""
          ),
          Row(
            "<http://example.org/carol>",
            "\"Carol Baz\""
          ),
          Row(
            "<http://example.org/carol>",
            "\"C. Baz\""
          ),
          Row(
            "<http://example.org/alice>",
            "\"Alice Foo\""
          ),
          Row(
            "<http://example.org/alice>",
            "\"A. Foo\""
          ),
          Row(
            "<http://example.org/bob>",
            "\"B. Bar\""
          ),
          Row(
            "<http://example.org/bob>",
            "\"Bob\""
          ),
          Row(
            "<http://example.org/bob>",
            "\"Bob Bar\""
          )
        )
      }

      "other cases" when {

        "execute and obtain expected results from sub-query with SELECT and GROUP BY" in {

          val df: DataFrame = List(
            (
              "<http://people.example/alice>",
              "<http://people.example/name>",
              "Alice Foo",
              ""
            ),
            (
              "<http://people.example/alice>",
              "<http://people.example/name>",
              "A. Foo",
              ""
            ),
            (
              "<http://people.example/alice>",
              "<http://people.example/knows>",
              "<http://people.example/bob>",
              ""
            ),
            (
              "<http://people.example/alice>",
              "<http://people.example/knows>",
              "<http://people.example/carol>",
              ""
            ),
            (
              "<http://people.example/bob>",
              "<http://people.example/name>",
              "Bob",
              ""
            ),
            (
              "<http://people.example/bob>",
              "<http://people.example/name>",
              "Bob Bar",
              ""
            ),
            (
              "<http://people.example/bob>",
              "<http://people.example/name>",
              "B. Bar",
              ""
            ),
            (
              "<http://people.example/carol>",
              "<http://people.example/name>",
              "Carol",
              ""
            ),
            (
              "<http://people.example/carol>",
              "<http://people.example/name>",
              "Carol Baz",
              ""
            ),
            (
              "<http://people.example/carol>",
              "<http://people.example/name>",
              "C. Baz",
              ""
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX peop: <http://people.example/>
              |
              |SELECT ?y ?minName
              |WHERE {
              |  peop:alice peop:knows ?y .
              |  {
              |    SELECT ?y (MIN(?name) AS ?minName)
              |    WHERE {
              |      ?y peop:name ?name .
              |    } GROUP BY ?y
              |  }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect().length shouldEqual 2
          result.right.get.collect().toSet shouldEqual Set(
            Row("<http://people.example/bob>", "\"B. Bar\""),
            Row("<http://people.example/carol>", "\"C. Baz\"")
          )
        }
      }
    }
  }
}
