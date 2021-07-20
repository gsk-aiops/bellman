package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TypedLiteralsAndSameTermSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  lazy val df: DataFrame = List(
    (
      "<http://example.org/ns#a>",
      "<http://example.org/ns#p>",
      "\"value\""
    ),
    (
      "<http://example.org/ns#b>",
      "<http://example.org/ns#p>",
      "\"value\"^^<http://www.w3.org/2001/XMLSchema#string>"
    ),
    (
      "<http://example.org/ns#c>",
      "<http://example.org/ns#p>",
      "\"value\"^^<http://example.org/datatype#datatype>"
    ),
    (
      "<http://example.org/ns#d>",
      "<http://example.org/ns#p>",
      "\"value\"@en"
    ),
    (
      "<http://example.org/ns#e>",
      "<http://example.org/ns#p>",
      "\"3\"^^xsd:integer"
    ),
    (
      "<http://example.org/ns#f>",
      "<http://example.org/ns#p>",
      "\"3\""
    )
  ).toDF("s", "p", "o")

  val expected: List[Row] =
    List("<http://example.org/ns#a>", "<http://example.org/ns#b>").map(Row(_))

  val projection: Option[Column] = None

  "query 1" in {

    val query =
      """
        |PREFIX ns:   <http://example.org/ns#>
        |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
        |
        |SELECT ?x
        |WHERE { ?x ns:p "value" }
        |""".stripMargin

    Evaluation.eval(
      df,
      projection,
      query,
      expected
    )
  }

  "query 2" in {

    val query =
      """
        |PREFIX ns:   <http://example.org/ns#>
        |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
        |
        |SELECT ?x
        |WHERE { ?x ns:p "value"^^xsd:string }
        |""".stripMargin

    Evaluation.eval(
      df,
      projection,
      query,
      expected
    )
  }

  "query 3" in {

    val query =
      """
        |PREFIX ns:   <http://example.org/ns#>
        |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
        |
        |SELECT ?x
        |WHERE {
        |   ?x ns:p ?y
        |   FILTER (?y = "value")
        |}
        |""".stripMargin

    Evaluation.eval(
      df,
      projection,
      query,
      expected
    )
  }

  "query 4" in {

    val query =
      """
        |PREFIX ns:   <http://example.org/ns#>
        |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
        |
        |SELECT ?x
        |WHERE {
        |   ?x ns:p ?y
        |   FILTER (?y = "value"^^xsd:string)
        |}
        |""".stripMargin

    Evaluation.eval(
      df,
      projection,
      query,
      expected
    )
  }

  "query 5" in {

    val query =
      """
        |PREFIX ns:   <http://example.org/ns#>
        |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
        |
        |SELECT ?x
        |WHERE {
        |   ?x ns:p ?y
        |   FILTER (sameTerm(?y, "value"))
        |}
        |""".stripMargin

    Evaluation.eval(
      df,
      projection,
      query,
      expected
    )
  }

  "query 6" in {

    val query =
      """
        |PREFIX ns:   <http://example.org/ns#>
        |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
        |
        |SELECT ?x 
        |WHERE { 
        |   ?x ns:p ?y 
        |   FILTER (sameTerm(?y, "value"^^xsd:string)) 
        |}
        |""".stripMargin

    Evaluation.eval(
      df,
      projection,
      query,
      expected
    )
  }

  "query 7" in {

    val query =
      """
        |PREFIX ns:   <http://example.org/ns#>
        |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
        |
        |SELECT ?x 
        |WHERE { 
        |   ?x ns:p ?y 
        |   FILTER (sameTerm(?y, 3)) 
        |}
        |""".stripMargin

    val expected: List[Row] =
      List(Row("<http://example.org/ns#e>"))

    Evaluation.eval(
      df,
      projection,
      query,
      expected
    )
  }

}
