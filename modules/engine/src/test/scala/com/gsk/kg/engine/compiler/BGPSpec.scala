package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class BGPSpec extends AnyWordSpec with Matchers with SparkSpec with TestConfig {

  import sqlContext.implicits._

  val dfList = List(
    (
      "test",
      "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
      "<http://id.gsk.com/dm/1.0/Document>",
      ""
    ),
    ("test", "<http://id.gsk.com/dm/1.0/docSource>", "source", "")
  )

  "perform query with BGPs" should {

    "will execute operations in the dataframe" in {

      val df = dfList.toDF("s", "p", "o", "g")
      val query =
        """
            SELECT
              ?s ?p ?o
            WHERE {
              ?s ?p ?o .
            }
            """

      Compiler
        .compile(df, query, config)
        .right
        .get
        .collect() shouldEqual Array(
        Row(
          "\"test\"",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://id.gsk.com/dm/1.0/Document>"
        ),
        Row("\"test\"", "<http://id.gsk.com/dm/1.0/docSource>", "\"source\"")
      )
    }

    "will execute with two dependent BGPs" in {

      val df: DataFrame = dfList.toDF("s", "p", "o", "g")

      val query =
        """
            SELECT
              ?d ?src
            WHERE {
              ?d a <http://id.gsk.com/dm/1.0/Document> .
              ?d <http://id.gsk.com/dm/1.0/docSource> ?src
            }
            """

      Compiler
        .compile(df, query, config)
        .right
        .get
        .collect() shouldEqual Array(
        Row("\"test\"", "\"source\"")
      )
    }
  }

  "BGP query on string literals" should {

    "return expected results" when {

      "plain literal on query and plain literal on dataframe" in {

        val df: DataFrame = List(
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Alice"
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Bob"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?s
            |WHERE {
            | ?s foaf:name "Alice" .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect().length shouldEqual 1
        result.right.get.collect().toSet shouldEqual Set(
          Row("<http://example.org/alice>")
        )
      }

      "plain literal on query and plain literal on dataframe with extra double quotes" in {

        val df: DataFrame = List(
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/name>",
            "\"Alice\""
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/name>",
            "\"Bob\""
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?s
            |WHERE {
            | ?s foaf:name "Alice" .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect().length shouldEqual 1
        result.right.get.collect().toSet shouldEqual Set(
          Row("<http://example.org/alice>")
        )
      }

      "plain literal on query and typed literal on dataframe" in {

        val df: DataFrame = List(
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/name>",
            "\"Alice\"^^<http://www.w3.org/2001/XMLSchema#string>"
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/name>",
            "\"Bob\"^^<http://www.w3.org/2001/XMLSchema#string>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            |
            |SELECT ?s
            |WHERE {
            | ?s foaf:name "Alice" .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect().length shouldEqual 1
        result.right.get.collect().toSet shouldEqual Set(
          Row("<http://example.org/alice>")
        )
      }

      "typed literal on query and plain literal on dataframe" in {

        val df: DataFrame = List(
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Alice"
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Bob"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            |
            |SELECT ?s
            |WHERE {
            | ?s foaf:name "Alice"^^xsd:string .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect().length shouldEqual 1
        result.right.get.collect().toSet shouldEqual Set(
          Row("<http://example.org/alice>")
        )
      }

      "typed literal on query and plain literal on dataframe with extra double quotes" in {

        val df: DataFrame = List(
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/name>",
            "\"Alice\""
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/name>",
            "\"Bob\""
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            |
            |SELECT ?s
            |WHERE {
            | ?s foaf:name "Alice"^^xsd:string .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect().length shouldEqual 1
        result.right.get.collect().toSet shouldEqual Set(
          Row("<http://example.org/alice>")
        )
      }

      "typed literal on query and plain typed literal on dataframe" in {

        val df: DataFrame = List(
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/name>",
            "\"Alice\"^^<http://www.w3.org/2001/XMLSchema#string>"
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/name>",
            "\"Bob\"^^<http://www.w3.org/2001/XMLSchema#string>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            |
            |SELECT ?s
            |WHERE {
            | ?s foaf:name "Alice"^^xsd:string .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect().length shouldEqual 1
        result.right.get.collect().toSet shouldEqual Set(
          Row("<http://example.org/alice>")
        )
      }
    }
  }

  "BGP query on boolean literals" should {

    "return expected results" when {

      "plain literal on query and plain literal on dataframe" in {

        val df: DataFrame = List(
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/isFriend>",
            true
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/isFriend>",
            false
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?s
            |WHERE
            |{
            | ?s foaf:isFriend false .     
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect().length shouldEqual 1
        result.right.get.collect().toSet shouldEqual Set(
          Row("<http://example.org/bob>")
        )
      }

      "plain literal on query and typed literal on dataframe" in {

        val df: DataFrame = List(
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/isFriend>",
            "\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>"
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/isFriend>",
            "\"false\"^^<http://www.w3.org/2001/XMLSchema#boolean>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            |
            |SELECT ?s
            |WHERE
            |{
            | ?s foaf:isFriend false .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect().length shouldEqual 1
        result.right.get.collect().toSet shouldEqual Set(
          Row("<http://example.org/bob>")
        )
      }

      "typed literal on query and plain literal on dataframe" in {

        val df: DataFrame = List(
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/isFriend>",
            true
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/isFriend>",
            false
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            |
            |SELECT ?s
            |WHERE
            |{
            | ?s foaf:isFriend "false"^^xsd:boolean .     
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect().length shouldEqual 1
        result.right.get.collect().toSet shouldEqual Set(
          Row("<http://example.org/bob>")
        )
      }

      "typed literal on query and typed literal on dataframe" in {}
    }
  }
}
