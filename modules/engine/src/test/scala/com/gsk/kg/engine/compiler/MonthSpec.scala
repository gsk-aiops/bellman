package com.gsk.kg.engine.compiler

import com.gsk.kg.sparqlparser.TestConfig
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MonthSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  /*
  https://www.w3.org/TR/sparql11-query/#func-month
  YEAR("2011-01-10T14:45:13.815-05:00"^^xsd:dateTime) -> 1
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
      "\"2012-04-10T14:45:13.815-05:00\"^^xsd:dateTime"
    ),
    (
      "_:c",
      "<http://xmlns.com/foaf/0.1/date>",
      "\"2013-12-10T14:45:13.815-05:00\"^^xsd:dateTime"
    )
  ).toDF("s", "p", "o")

  val expected: List[Row] = List("1", "4", "12").map(Row(_))

  val projection: Option[Column] = None

  "perform year function correctly" when {
    "select year response with a year of dateTime value" in {

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT MONTH(?date)
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

    "bind year response with a year value" in {

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT ?m
          |WHERE  {
          |   ?x foaf:date ?date .
          |   bind(month(?date) as ?m)
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
