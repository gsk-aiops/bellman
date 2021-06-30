package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.to_timestamp

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class NowSpec extends AnyWordSpec with Matchers with SparkSpec with TestConfig {

  import sqlContext.implicits._

  /*
  https://www.w3.org/TR/sparql11-query/#func-now
  NOW() -> "2011-01-10T14:45:13.815-05:00"^^xsd:dateTime
   */

  lazy val df: DataFrame = List(
    ("_:a", "<http://xmlns.com/foaf/0.1/name>", "Alice", ""),
    ("_:b", "<http://xmlns.com/foaf/0.1/name>", "Bob", ""),
    ("_:c", "<http://xmlns.com/foaf/0.1/name>", "Alice", "")
  ).toDF("s", "p", "o", "g")

  "perform now function correctly" when {
    "select now response with an xsd:dateTime valid" in {

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT NOW()
          |WHERE  {
          |   ?x foaf:name ?name
          |}
          |""".stripMargin

      evaluate(df, query)
    }

    "bind now response with an xsd:dateTime valid" in {

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT ?d
          |WHERE  {
          |   ?x foaf:name ?name .
          |   bind(now() as ?d)
          |}
          |""".stripMargin

      evaluate(df, query)
    }
  }
  private def evaluate(df: DataFrame, query: String): Assertion = {

    val startPos = 2
    val len      = 29

    val result = Compiler.compile(df, query, config)

    val dfR: DataFrame = result match {
      case Left(e)  => throw new Exception(e.toString)
      case Right(r) => r
    }
    dfR.select(col(dfR.columns.head).as("now")).show(false)
    dfR
      .select(
        to_timestamp(col(dfR.columns.head).substr(startPos, len))
      )
      .show(false)
    val expected = Set(Row(true))
    dfR
      .select(
        to_timestamp(col(dfR.columns.head).substr(startPos, len)).isNotNull
      )
      .collect()
      .toSet shouldEqual expected
  }
}
