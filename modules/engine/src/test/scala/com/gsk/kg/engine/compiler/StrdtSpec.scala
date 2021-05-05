package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class StrdtSpec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
    with TestConfig {

  import sqlContext.implicits._

  "perform STRDT function correctly" should {

    "execute and obtain expected result with URI" in {
      val df = List(
        (
          "usa",
          "http://xmlns.com/foaf/0.1/latitude",
          "123"
        ),
        (
          "usa",
          "http://xmlns.com/foaf/0.1/longitude",
          "456"
        ),
        (
          "spain",
          "http://xmlns.com/foaf/0.1/latitude",
          "789"
        ),
        (
          "spain",
          "http://xmlns.com/foaf/0.1/longitude",
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
          "http://xmlns.com/foaf/0.1/latitude",
          "123"
        ),
        (
          "usa",
          "http://xmlns.com/foaf/0.1/longitude",
          "456"
        ),
        (
          "spain",
          "http://xmlns.com/foaf/0.1/latitude",
          "789"
        ),
        (
          "spain",
          "http://xmlns.com/foaf/0.1/longitude",
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

    // TODO: Un-ignore when CONCAT with multiple arguments
    // See: https://github.com/gsk-aiops/bellman/issues/324
    "execute and obtain expected results when complex expression" ignore {
      val df = List(
        (
          "http://example.org/usa",
          "http://xmlns.com/foaf/0.1/latitude",
          "123"
        ),
        (
          "http://example.org/usa",
          "http://xmlns.com/foaf/0.1/longitude",
          "456"
        ),
        (
          "http://example.org/spain",
          "http://xmlns.com/foaf/0.1/latitude",
          "789"
        ),
        (
          "http://example.org/spain",
          "http://xmlns.com/foaf/0.1/latitude",
          "901"
        )
      ).toDF("s", "p", "o")

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT ?coords
          |WHERE {
          |  ?country foaf:latitude ?lat .
          |  ?country foaf:longitude ?long .
          |  BIND(STRDT(CONCAT("country=", strafter(?country, "http://xmlns.com/foaf/0.1/"), "&long=", str(?long), "&lat=", str(?lat)), <http://geo.org/coords>) as ?coords)
          |}
          |""".stripMargin

      val result = Compiler.compile(df, query, config)

      result.right.get.collect.length shouldEqual 2
      result.right.get.collect.toSet shouldEqual Set(
        Row("\"country=usa&long=123&lat=456\"^^<http://geo.org/coords>"),
        Row("\"country=spain&long=789&lat=012\"^^<http://geo.org/coords>")
      )
    }
  }
}
