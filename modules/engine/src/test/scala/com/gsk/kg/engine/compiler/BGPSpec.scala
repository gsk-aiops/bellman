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
}
