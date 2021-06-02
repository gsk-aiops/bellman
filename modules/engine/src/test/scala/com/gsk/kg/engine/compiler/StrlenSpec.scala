package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class StrlenSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "perform STRLEN function correctly" when {

    "on variable mixing string types" in {

      val df = List(
        (
          "_:alice",
          "<http://xmlns.com/foaf/0.1/name>",
          "Alice"
        ),
        (
          "_:alice",
          "<http://xmlns.com/foaf/0.1/name>",
          "\"Alice\"^^xsd:string"
        ),
        (
          "_:alice",
          "<http://xmlns.com/foaf/0.1/name>",
          "\"Alice\"@en"
        ),
        (
          "_:bob",
          "<http://xmlns.com/foaf/0.1/name>",
          "Bob"
        ),
        (
          "_:bob",
          "<http://xmlns.com/foaf/0.1/name>",
          "\"Bob\"^^xds:string"
        ),
        (
          "_:bob",
          "<http://xmlns.com/foaf/0.1/name>",
          "\"Bob\"@en"
        )
      ).toDF("s", "p", "o")

      val query =
        """
          |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
          |
          |SELECT  ?name
          |WHERE   {
          |   ?x foaf:name ?name .
          |   FILTER(strlen(?name) > 4)
          |}
          |""".stripMargin

      val result = Compiler.compile(df, query, config)

      result.right.get.collect.length shouldEqual 3
      result.right.get.collect.toSet shouldEqual Set(
        Row("\"Alice\""),
        Row("\"Alice\"^^xsd:string"),
        Row("\"Alice\"@en")
      )
    }
  }
}
