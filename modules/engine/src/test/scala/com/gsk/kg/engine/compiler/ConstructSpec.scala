package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ConstructSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

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

  "perform with CONSTRUCT statement" should {

    "execute with a single triple pattern" in {

      val df: DataFrame = dfList.toDF("s", "p", "o", "g")

      val query =
        """
        CONSTRUCT {
          ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o
        } WHERE {
          ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o
        }
        """

      Compiler
        .compile(df, query, config)
        .right
        .get
        .collect
        .toSet shouldEqual Set(
        Row(
          "\"test\"",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://id.gsk.com/dm/1.0/Document>"
        )
      )
    }

    "execute with more than one triple pattern" in {

      val positive = List(
        (
          "doesmatch",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://id.gsk.com/dm/1.0/Document>",
          ""
        ),
        (
          "doesmatchaswell",
          "<http://id.gsk.com/dm/1.0/docSource>",
          "potato",
          ""
        )
      )
      val df: DataFrame = (positive ++ dfList).toDF("s", "p", "o", "g")

      val query =
        """
          |CONSTRUCT {
          |  ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o .
          |  ?s2 <http://id.gsk.com/dm/1.0/docSource> ?o2
          |} WHERE {
          |  ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o .
          |  ?s2 <http://id.gsk.com/dm/1.0/docSource> ?o2
          |}
          |""".stripMargin

      val result =
        Compiler.compile(df, query, config).right.get.collect().toSet
      result shouldEqual Set(
        Row(
          "\"doesmatch\"",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://id.gsk.com/dm/1.0/Document>"
        ),
        Row(
          "\"doesmatchaswell\"",
          "<http://id.gsk.com/dm/1.0/docSource>",
          "\"potato\""
        ),
        Row(
          "\"test\"",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://id.gsk.com/dm/1.0/Document>"
        ),
        Row(
          "\"test\"",
          "<http://id.gsk.com/dm/1.0/docSource>",
          "\"source\""
        )
      )
    }

    "execute with more than one triple pattern with common bindings" in {

      val negative = List(
        (
          "doesntmatch",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://id.gsk.com/dm/1.0/Document>",
          ""
        ),
        (
          "doesntmatcheither",
          "<http://id.gsk.com/dm/1.0/docSource>",
          "potato",
          ""
        )
      )

      val df: DataFrame = (negative ++ dfList).toDF("s", "p", "o", "g")

      val query =
        """
      CONSTRUCT
      {
        ?d <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://id.gsk.com/dm/1.0/Document> .
        ?d <http://id.gsk.com/dm/1.0/docSource> ?src
      }
      WHERE
      {
        ?d <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://id.gsk.com/dm/1.0/Document> .
        ?d <http://id.gsk.com/dm/1.0/docSource> ?src
      }
      """

      Compiler
        .compile(df, query, config)
        .right
        .get
        .collect()
        .toSet shouldEqual Set(
        Row(
          "\"test\"",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://id.gsk.com/dm/1.0/Document>"
        ),
        Row(
          "\"test\"",
          "<http://id.gsk.com/dm/1.0/docSource>",
          "\"source\""
        )
      )
    }
  }

  "perform query with CONSTRUCT statement" should {

    "execute and apply default ordering CONSTRUCT queries" in {

      val df: DataFrame = List(
        (
          "<http://potato.com/b>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/docSource>",
          "<http://thesour.ce>",
          ""
        ),
        (
          "<http://potato.com/c>",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>",
          ""
        ),
        (
          "<http://potato.com/c>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/docSource>",
          "<http://thesour.ce>",
          ""
        ),
        (
          "<http://potato.com/d>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/docSource>",
          "<http://thesour.ce>",
          ""
        ),
        (
          "<http://potato.com/b>",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>",
          ""
        ),
        (
          "<http://potato.com/d>",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>",
          ""
        ),
        ("negative", "negative", "negative", "")
      ).toDF("s", "p", "o", "g")

      val query =
        """
            PREFIX  dm:   <http://gsk-kg.rdip.gsk.com/dm/1.0/>

            CONSTRUCT {
             ?d a dm:Document .
             ?d dm:docSource ?src .
            }
            WHERE{
             ?d a dm:Document .
             ?d dm:docSource ?src .
            }
            """

      val result = Compiler.compile(df, query, config)

      result shouldBe a[Right[_, _]]
      result.right.get.collect.length shouldEqual 6
      result.right.get.collect.toSet shouldEqual Set(
        Row(
          "<http://potato.com/b>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/docSource>",
          "<http://thesour.ce>"
        ),
        Row(
          "<http://potato.com/b>",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>"
        ),
        Row(
          "<http://potato.com/c>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/docSource>",
          "<http://thesour.ce>"
        ),
        Row(
          "<http://potato.com/c>",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>"
        ),
        Row(
          "<http://potato.com/d>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/docSource>",
          "<http://thesour.ce>"
        ),
        Row(
          "<http://potato.com/d>",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>"
        )
      )
    }

    "execute and make sense in the LIMIT cause when there's no ORDER BY" in {

      val df: DataFrame = List(
        (
          "<http://potato.com/b>",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>",
          ""
        ),
        (
          "<http://potato.com/c>",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>",
          ""
        ),
        (
          "<http://potato.com/b>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/docSource>",
          "<http://thesour.ce>",
          ""
        ),
        (
          "<http://potato.com/d>",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>",
          ""
        ),
        (
          "<http://potato.com/c>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/docSource>",
          "<http://thesour.ce>",
          ""
        ),
        (
          "<http://potato.com/d>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/docSource>",
          "<http://thesour.ce>",
          ""
        ),
        ("negative", "negative", "negative", "")
      ).toDF("s", "p", "o", "g")

      val query =
        """
      PREFIX  dm:   <http://gsk-kg.rdip.gsk.com/dm/1.0/>

      CONSTRUCT {
       ?d a dm:Document .
       ?d dm:docSource ?src .
      }
      WHERE{
       ?d a dm:Document .
       ?d dm:docSource ?src .
      }
      LIMIT 1
      """

      val result = Compiler.compile(df, query, config)

      result shouldBe a[Right[_, _]]
      result.right.get.collect.length shouldEqual 2
      result.right.get.collect.toSet shouldEqual Set(
        Row(
          "<http://potato.com/b>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/docSource>",
          "<http://thesour.ce>"
        ),
        Row(
          "<http://potato.com/b>",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>"
        )
      )
    }

    "execute and work correctly with blank nodes in templates with a single blank label" in {

      val df: DataFrame = List(
        (
          "<http://potato.com/b>",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>",
          ""
        ),
        (
          "<http://potato.com/c>",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>",
          ""
        ),
        (
          "<http://potato.com/b>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/docSource>",
          "<http://thesour.ce>",
          ""
        ),
        (
          "<http://potato.com/d>",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>",
          ""
        ),
        (
          "<http://potato.com/c>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/docSource>",
          "<http://thesour.ce>",
          ""
        ),
        (
          "<http://potato.com/d>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/docSource>",
          "<http://thesour.ce>",
          ""
        )
      ).toDF("s", "p", "o", "g")

      val query =
        """
      PREFIX  dm:   <http://gsk-kg.rdip.gsk.com/dm/1.0/>

      CONSTRUCT {
       _:asdf a dm:Document .
       _:asdf dm:docSource ?src .
      }
      WHERE{
       ?d a dm:Document .
       ?d dm:docSource ?src .
      }
      """

      val result = Compiler.compile(df, query, config)

      val arrayResult = result.right.get.collect
      result shouldBe a[Right[_, _]]
      arrayResult should have size 6
      arrayResult.map(_.get(0)).distinct should have size 3
      arrayResult.map(row => (row.get(1), row.get(2))).toSet shouldEqual Set(
        (
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/docSource>",
          "<http://thesour.ce>"
        ),
        (
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>"
        ),
        (
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/docSource>",
          "<http://thesour.ce>"
        ),
        (
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>"
        ),
        (
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/docSource>",
          "<http://thesour.ce>"
        ),
        (
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>"
        )
      )
    }

    "execute and work correctly with blank nodes in templates with more than one blank label" in {

      val df: DataFrame = List(
        (
          "<http://potato.com/b>",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>",
          ""
        ),
        (
          "<http://potato.com/c>",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>",
          ""
        ),
        (
          "<http://potato.com/b>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/docSource>",
          "<http://thesour.ce>",
          ""
        ),
        (
          "<http://potato.com/d>",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>",
          ""
        ),
        (
          "<http://potato.com/c>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/docSource>",
          "<http://thesour.ce>",
          ""
        ),
        (
          "<http://potato.com/d>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/docSource>",
          "<http://thesour.ce>",
          ""
        )
      ).toDF("s", "p", "o", "g")

      val query =
        """
      PREFIX  dm:   <http://gsk-kg.rdip.gsk.com/dm/1.0/>

      CONSTRUCT {
       _:asdf a dm:Document .
       _:asdf dm:linksInSomeWay _:qwer .
       _:qwer dm:source ?src .
      }
      WHERE{
       ?d a dm:Document .
       ?d dm:docSource ?src .
      }
      """

      val result      = Compiler.compile(df, query, config)
      val resultDF    = result.right.get
      val arrayResult = resultDF.collect

      result shouldBe a[Right[_, _]]
      arrayResult should have size 9
      arrayResult.map(_.get(0)).distinct should have size 6
    }

    "execute and return no duplicates when input dataframe has duplicates" in {

      val df: DataFrame = List(
        (
          "<http://example.org#doc1>",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>"
        ),
        (
          "<http://example.org#doc1>",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>"
        )
      ).toDF("s", "p", "o")

      val query =
        """
          |PREFIX dm: <http://gsk-kg.rdip.gsk.com/dm/1.0/>
          |PREFIX litn: <http://lit-search-api/node/>
          |PREFIX litp: <http://lit-search-api/property/>
          |
          |CONSTRUCT {
          | ?Document a litn:Document .
          | ?Document litp:docID ?docid .
          |}
          |WHERE {
          | ?d a dm:Document .
          | BIND(STRAFTER(str(?d), "#") as ?docid) .
          | BIND(URI(CONCAT("http://lit-search-api/node/doc#", ?docid)) as ?Document) .
          |}
          |""".stripMargin

      val result = Compiler.compile(df, query, config)

      result.right.get.collect().length shouldEqual 2
      result.right.get.collect().toSet shouldEqual Set(
        Row(
          "<http://lit-search-api/node/doc#doc1>",
          "<http://lit-search-api/property/docID>",
          "\"doc1\""
        ),
        Row(
          "<http://lit-search-api/node/doc#doc1>",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://lit-search-api/node/Document>"
        )
      )
    }
  }

  "construct on numeric literals" in {

    val df: DataFrame = List(
      (
        "<http://example.org/alice>",
        "<http://xmlns.com/foaf/0.1/age>",
        "\"28\"^^<http://www.w3.org/2001/XMLSchema#int>"
      ),
      (
        "<http://example.org/bob>",
        "<http://xmlns.com/foaf/0.1/age>",
        "\"17\"^^xsd:decimal"
      ),
      (
        "<http://example.org/charlie>",
        "<http://xmlns.com/foaf/0.1/age>",
        "35"
      )
    ).toDF("s", "p", "o")

    df.show(false)

    val query =
      """
        |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        |
        |CONSTRUCT {
        |  ?s foaf:age2 ?n .
        |} WHERE {
        |  ?s foaf:age ?n .
        |}
        |""".stripMargin

    val result = Compiler.compile(df, query, config)

    result.right.get.show(false)

    result shouldBe a[Right[_, _]]
    result.right.get.collect.length shouldEqual 2
    result.right.get.collect.toSet shouldEqual Set(
      Row(
        "<http://example.org/charlie>",
        "<http://xmlns.com/foaf/0.1/age2>",
        "35"
      ),
      Row(
        "<http://example.org/alice>",
        "<http://xmlns.com/foaf/0.1/age2>",
        "\"28\"^^<http://www.w3.org/2001/XMLSchema#int>"
      )
    )
  }
}
