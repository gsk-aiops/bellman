package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class UnionSpec
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

  "perform query with UNION statement" should {

    "execute with the same bindings" in {

      val df: DataFrame = (("does", "not", "match", "") :: dfList)
        .toDF("s", "p", "o", "g")

      val query =
        """
      SELECT
        ?s ?o
      WHERE {
        { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o }
        UNION
        { ?s <http://id.gsk.com/dm/1.0/docSource> ?o }
      }
      """

      Compiler
        .compile(df, query, config)
        .right
        .get
        .collect() shouldEqual Array(
        Row("\"test\"", "<http://id.gsk.com/dm/1.0/Document>"),
        Row("\"test\"", "\"source\"")
      )
    }

    "execute with different bindings" in {

      val df: DataFrame =
        (("does", "not", "match", "") :: dfList).toDF("s", "p", "o", "g")

      val query =
        """
      SELECT
        ?s ?o ?s2 ?o2
      WHERE {
        { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o }
        UNION
        { ?s2 <http://id.gsk.com/dm/1.0/docSource> ?o2 }
      }
      """

      Compiler
        .compile(df, query, config)
        .right
        .get
        .collect() shouldEqual Array(
        Row("\"test\"", "<http://id.gsk.com/dm/1.0/Document>", null, null),
        Row(null, null, "\"test\"", "\"source\"")
      )
    }
  }
}
