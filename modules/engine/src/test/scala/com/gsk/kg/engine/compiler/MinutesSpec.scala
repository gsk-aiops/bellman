package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MinutesSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  /*
  https://www.w3.org/TR/sparql11-query/#func-minutes
  MINUTES("2011-01-10T14:45:13.815-05:00"^^xsd:dateTime) -> 45
   */

  lazy val df: DataFrame = List(
    (
      "_:a",
      "<http://xmlns.com/foaf/0.1/date>",
      "\"2011-01-10T14:45:13.815-05:00\"^^xsd:dateTime"
    ),
    (
      "_:b",
      "<http://xmlns.com/foaf/0.1/date>",
      "\"2012-04-14T14:38:13.815-05:00\"^^xsd:dateTime"
    ),
    (
      "_:c",
      "<http://xmlns.com/foaf/0.1/date>",
      "\"2013-12-09T14:09:13.815-05:00\"^^xsd:dateTime"
    )
  ).toDF("s", "p", "o")

  val expected: List[Row] = List("45", "38", "9").map(Row(_))

  val projection: Option[Column] = None

  "perform minutes function correctly" when {
    "select minutes response with a minutes of dateTime value" in {

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT MINUTES(?date)
          |WHERE  {
          |   ?x foaf:date ?date
          |}
          |""".stripMargin

      Evaluation.eval(
        df,
        projection,
        query,
        expected
      )
    }

    "bind minutes response with a minutes value" in {

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT ?m
          |WHERE  {
          |   ?x foaf:date ?date .
          |   bind(minutes(?date) as ?m)
          |}
          |""".stripMargin

      Evaluation.eval(
        df,
        projection,
        query,
        expected
      )
    }
  }

}
