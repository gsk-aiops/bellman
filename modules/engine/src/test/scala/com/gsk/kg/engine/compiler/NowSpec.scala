package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.functions.substring_index
import org.apache.spark.sql.functions.to_timestamp

import com.gsk.kg.sparqlparser.TestConfig

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

  val startPos            = 2
  val expected: List[Row] = (1 to 3).map(_ => Row(true)).toList

  val projection: Option[Column] = Some(
    to_timestamp(
      substring(
        substring_index(col(Evaluation.renamedColumn), "\"^^xsd:dateTime", 1),
        startPos,
        Int.MaxValue
      )
    ).isNotNull
  )

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

      Evaluation.eval(
        df,
        projection,
        query,
        expected
      )
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

      Evaluation.eval(
        df,
        projection,
        query,
        expected
      )
    }
  }
}
