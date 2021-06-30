package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class StrUuidSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  /*
  https://www.w3.org/TR/sparql11-query/#func-struuid
  STRUUID() -> b9302fb5-642e-4d3b-af19-29a8f6d894c9
   */

  lazy val df: DataFrame = List(
    ("_:a", "<http://xmlns.com/foaf/0.1/name>", "Alice", ""),
    ("_:b", "<http://xmlns.com/foaf/0.1/name>", "Bob", ""),
    ("_:c", "<http://xmlns.com/foaf/0.1/name>", "Alice", "")
  ).toDF("s", "p", "o", "g")

  val strUuidRegex =
    "[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}"
  val strUuidRegexColName = "uuidR"

  "perform struuid function correctly" when {
    "select struuid response with an UUID valid" in {

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT STRUUID()
          |WHERE  {
          |   ?x foaf:name ?name
          |}
          |""".stripMargin

      evaluate(df, query)
    }

    "bind struuid response with an UUID valid" in {

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT ?id
          |WHERE  {
          |   ?x foaf:name ?name .
          |   bind(struuid() as ?id)
          |}
          |""".stripMargin

      evaluate(df, query)
    }
  }
  private def evaluate(df: DataFrame, query: String): Assertion = {
    val result = Compiler.compile(df, query, config)

    val dfR: DataFrame = result match {
      case Left(e)  => throw new Exception(e.toString)
      case Right(r) => r
    }
    val expected = Set(Row(true))
    dfR
      .select(
        dfR(dfR.columns.head).rlike(strUuidRegex).as(strUuidRegexColName)
      )
      .collect()
      .toSet shouldEqual expected
  }

}
