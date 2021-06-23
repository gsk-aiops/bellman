package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class Ceilspec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  /*
  https://www.w3.org/TR/sparql11-query/#func-ceil
  CEIL(10.5)  -> 10
  CEIL(-10.5) -> -11
   */

  "perform ceil function correctly" when {

    "term is a simple numeric" in {
      val term     = "10.5"
      val expected = Row("11")
      eval(term, expected)
    }

    "term is NaN" in {
      val term     = "NaN"
      val expected = Row("0")
      eval(term, expected)
    }

    "term is not a numeric" in {
      val term     = "Hello"
      val expected = Row(null)
      eval(term, expected)
    }

    "term is xsd:double" in {
      val term     = "\"10.5\"^^xsd:string"
      val expected = Row("\"11\"^^xsd:string")
      eval(term, expected)
    }
  }

  private def eval(term: String, expected: Row): Assertion = {
    val df = List(
      (
        "<http://uri.com/subject/#a1>",
        "<http://xmlns.com/foaf/0.1/num>",
        term
      )
    ).toDF("s", "p", "o")

    val query =
      """
        |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        |
        |SELECT ceil(?num)
        |WHERE  {
        |   ?x foaf:num ?num
        |}
        |""".stripMargin

    val result = Compiler.compile(df, query, config)
    val dfR: DataFrame = result match {
      case Left(e)  => throw new Exception(e.toString)
      case Right(r) => r
    }

    dfR
      .collect()
      .head shouldEqual expected
  }

}
