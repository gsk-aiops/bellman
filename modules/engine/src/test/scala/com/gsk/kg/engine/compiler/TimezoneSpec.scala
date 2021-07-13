package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TimezoneSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  /*
  https://www.w3.org/TR/sparql11-query/#func-timezone
  MINUTES("2011-01-10T14:45:13.815-05:00"^^xsd:dateTime) -> "-PT5H"^^xsd:dayTimeDuration
   */

  lazy val df: DataFrame = List(
    (
      "_:a",
      "<http://xmlns.com/foaf/0.1/date>",
      "\"2011-01-10T14:45:13.815+05:00\"^^xsd:dateTime"
    ),
    (
      "_:b",
      "<http://xmlns.com/foaf/0.1/date>",
      "\"2012-04-14T14:38:13.815-05:00\"^^xsd:dateTime"
    ),
    (
      "_:c",
      "<http://xmlns.com/foaf/0.1/date>",
      "\"2013-12-09T14:09:13.815-05:11\"^^xsd:dateTime"
    )
  ).toDF("s", "p", "o")

  val expected: List[Row] = List(
    "\"PT5H\"^^xsd:dateTime",
    "\"-PT5H\"^^xsd:dateTime",
    "\"-PT5H11M\"^^xsd:dateTime"
  ).map(Row(_))

  val projection: Option[Column] = None

  "perform timezone function correctly" when {
    "select timezone response with a timezone of dateTime value" in {

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT TIMEZONE(?date)
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

    "bind timezone response with a timezone value" in {

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT ?m
          |WHERE  {
          |   ?x foaf:date ?date .
          |   bind(timezone(?date) as ?m)
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
