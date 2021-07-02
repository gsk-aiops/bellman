package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col

import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class UUIDSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  /*
  https://www.w3.org/TR/sparql11-query/#func-uuid
  UUID() -> <urn:uuid:b9302fb5-642e-4d3b-af19-29a8f6d894c9>
   */

  lazy val df: DataFrame = List(
    ("_:a", "<http://xmlns.com/foaf/0.1/name>", "Alice", ""),
    ("_:b", "<http://xmlns.com/foaf/0.1/name>", "Bob", ""),
    ("_:c", "<http://xmlns.com/foaf/0.1/name>", "Alice", "")
  ).toDF("s", "p", "o", "g")

  val uuidRegex =
    "urn:uuid:[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}"
  val uuidRegexColName = "uuidR"
  val expected         = (1 to 3).map(_ => Row(true)).toList

  "perform uuid function correctly" when {
    "select uuid response with an UUID valid" in {

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT UUID()
          |WHERE  {
          |   ?x foaf:name ?name
          |}
          |""".stripMargin

      Evaluation.eval(
        df,
        Some(col(Evaluation.renamedColumn).rlike(uuidRegex)),
        query,
        expected
      )
    }

    "bind uuid response with an UUID valid" in {

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT ?id
          |WHERE  {
          |   ?x foaf:name ?name .
          |   bind(uuid() as ?id)
          |}
          |""".stripMargin

      Evaluation.eval(
        df,
        Some(col(Evaluation.renamedColumn).rlike(uuidRegex)),
        query,
        expected
      )
    }
  }
}
