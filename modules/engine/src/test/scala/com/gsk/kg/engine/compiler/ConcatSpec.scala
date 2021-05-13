package com.gsk.kg.engine.compiler

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ConcatSpec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
    with TestConfig {

  import sqlContext.implicits._

  "CONCAT function" should {

    "execute and obtain expected results when complex expression" in {
      val df = List(
        (
          "\"USA\"",
          "<http://xmlns.com/foaf/0.1/latitude>",
          "123"
        ),
        (
          "\"USA\"",
          "<http://xmlns.com/foaf/0.1/longitude>",
          "456"
        ),
        (
          "\"Spain\"",
          "<http://xmlns.com/foaf/0.1/latitude>",
          "789"
        ),
        (
          "\"Spain\"",
          "<http://xmlns.com/foaf/0.1/longitude>",
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
          |  BIND(CONCAT("<http://geo.org/coords?country=", ?country, "&long=", str(?long), "&lat=", str(?lat), ">") as ?coords)
          |}
          |""".stripMargin

      val result = Compiler.compile(df, query, config)

      result.right.get.collect.length shouldEqual 2
      result.right.get.collect.toSet shouldEqual Set(
        Row("<http://geo.org/coords?country=USA&long=456&lat=123>"),
        Row("<http://geo.org/coords?country=Spain&long=901&lat=789>")
      )
    }
  }
}
