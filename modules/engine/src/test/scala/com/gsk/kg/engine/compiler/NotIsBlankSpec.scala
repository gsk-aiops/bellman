package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class NotIsBlankSpec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
    with TestConfig {

  import sqlContext.implicits._

  "perform query with !ISBLANK function" should {

    "execute and obtain expected results" in {

      val df: DataFrame = List(
        ("_:a", "<http://xmlns.com/foaf/0.1/name>", "Alice", ""),
        (
          "_:a",
          "<http://xmlns.com/foaf/0.1/mbox>",
          "<mailto:alice@work.example>",
          ""
        ),
        ("_:b", "<http://xmlns.com/foaf/0.1/name>", "_:bob", ""),
        (
          "_:b",
          "<http://xmlns.com/foaf/0.1/mbox>",
          "<mailto:bob@work.example>",
          ""
        )
      ).toDF("s", "p", "o", "g")

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT ?name ?mbox
          |WHERE {
          |   ?x foaf:name ?name ;
          |      foaf:mbox  ?mbox .
          |   FILTER (!isBlank(?name))
          |}
          |""".stripMargin

      val result = Compiler.compile(df, query, config)

      result shouldBe a[Right[_, _]]
      result.right.get.collect.length shouldEqual 1
      result.right.get.collect shouldEqual Array(
        Row("\"Alice\"", "<mailto:alice@work.example>")
      )
    }
  }
}
