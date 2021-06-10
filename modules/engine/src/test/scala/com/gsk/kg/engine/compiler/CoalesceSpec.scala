package com.gsk.kg.engine.compiler

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CoalesceSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "perform query with COALESCE function" when {

    "simple query" in {

      val df: DataFrame = List(
        (
          "<http://example.org/book1>",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://purl.org/dc/dcmitype/PhysicalObject>"
        ),
        (
          "<http://example.org/book1>",
          "<http://purl.org/dc/elements/1.1/title>",
          "SPARQL 1.1 Tutorial"
        ),
        (
          "<http://example.org/book2>",
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "<http://purl.org/dc/dcmitype/PhysicalObject>"
        )
      ).toDF("s", "p", "o")

      val query =
        """
          |PREFIX dc: <http://purl.org/dc/elements/1.1/>
          |PREFIX dcmitype: <http://purl.org/dc/dcmitype/>
          |
          |SELECT ?book (COALESCE(?title, "Not available") AS ?bookTitle)
          |WHERE {
          |  ?book a dcmitype:PhysicalObject .
          |  OPTIONAL {
          |    ?book dc:title ?title .
          |  }
          |}
          |""".stripMargin

      val result = Compiler.compile(df, query, config).right.get

      result.collect.length shouldEqual 2
      result.collect.toSet shouldEqual Set(
        Row("<http://example.org/book1>", "\"SPARQL 1.1 Tutorial\""),
        Row("<http://example.org/book2>", "\"Not available\"")
      )
    }
  }
}
