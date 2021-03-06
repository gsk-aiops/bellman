package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class StrdtSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "perform STRDT function correctly" should {

    "execute on SELECT statement" in {
      val df = List(
        (
          "Peter",
          "<http://xmlns.com/foaf/0.1/age>",
          "21"
        )
      ).toDF("s", "p", "o")

      val query =
        """
          |PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
          |
          |SELECT ?o strdt("123", xsd:integer)
          |WHERE { ?x ?t ?o . }
          |""".stripMargin

      val result = Compiler.compile(df, query, config)

      result.right.get.collect.length shouldEqual 1
      result.right.get.collect.toSet shouldEqual Set(
        Row("21", "\"123\"^^<http://www.w3.org/2001/XMLSchema#integer>")
      )
    }

    "execute and obtain expected result with URI" in {
      val df = List(
        (
          "usa",
          "<http://xmlns.com/foaf/0.1/latitude>",
          "123"
        ),
        (
          "usa",
          "<http://xmlns.com/foaf/0.1/longitude>",
          "456"
        ),
        (
          "spain",
          "<http://xmlns.com/foaf/0.1/latitude>",
          "789"
        ),
        (
          "spain",
          "<http://xmlns.com/foaf/0.1/longitude>",
          "901"
        )
      ).toDF("s", "p", "o")

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT ?country
          |WHERE {
          |  ?c foaf:latitude ?lat .
          |  ?c foaf:longitude ?long .
          |  BIND(strdt(?c, <http://geo.org#country>) as ?country)
          |}
          |""".stripMargin

      val result = Compiler.compile(df, query, config)

      result.right.get.collect.length shouldEqual 2
      result.right.get.collect.toSet shouldEqual Set(
        Row("\"usa\"^^<http://geo.org#country>"),
        Row("\"spain\"^^<http://geo.org#country>")
      )
    }

    "execute and obtain expected result with URI prefix" in {
      val df = List(
        (
          "usa",
          "<http://xmlns.com/foaf/0.1/latitude>",
          "123"
        ),
        (
          "usa",
          "<http://xmlns.com/foaf/0.1/longitude>",
          "456"
        ),
        (
          "spain",
          "<http://xmlns.com/foaf/0.1/latitude>",
          "789"
        ),
        (
          "spain",
          "<http://xmlns.com/foaf/0.1/longitude>",
          "901"
        )
      ).toDF("s", "p", "o")

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |PREFIX geo: <http://geo.org#>
          |
          |SELECT ?country
          |WHERE {
          |  ?c foaf:latitude ?lat .
          |  ?c foaf:longitude ?long .
          |  BIND(strdt(?c, geo:country) as ?country)
          |}
          |""".stripMargin

      val result = Compiler.compile(df, query, config)

      result.right.get.collect.length shouldEqual 2
      result.right.get.collect.toSet shouldEqual Set(
        Row("\"usa\"^^<http://geo.org#country>"),
        Row("\"spain\"^^<http://geo.org#country>")
      )
    }
  }
}
