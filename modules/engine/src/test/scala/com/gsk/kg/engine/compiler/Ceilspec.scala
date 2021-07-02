package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.sparqlparser.TestConfig

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

  def df(term: String): DataFrame = List(
    (
      "<http://uri.com/subject/#a1>",
      "<http://xmlns.com/foaf/0.1/num>",
      term
    )
  ).toDF("s", "p", "o")

  val query: String =
    """
      |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
      |
      |SELECT ceil(?num)
      |WHERE  {
      |   ?x foaf:num ?num
      |}
      |""".stripMargin

  val nullExpected: List[Row] = List(Row(null))

  "perform ceil function correctly" when {

    "term is a simple numeric" in {
      val term     = "10.5"
      val expected = List(Row("11"))
      Evaluation.eval(df(term), None, query, expected)
    }

    "term is NaN" in {
      val term     = "NaN"
      val expected = List(Row("0"))
      Evaluation.eval(df(term), None, query, expected)
    }

    "term is not a numeric" in {
      val term = "Hello"
      Evaluation.eval(df(term), None, query, nullExpected)
    }

    "term is xsd:double" in {
      val term     = "\"10.5\"^^xsd:double"
      val expected = List(Row("\"11\"^^xsd:double"))
      Evaluation.eval(df(term), None, query, expected)
    }

    "error when literal is typed but not numeric" in {
      val term = "\"10.5\"^^xsd:string"
      Evaluation.eval(df(term), None, query, nullExpected)
    }
  }
}
