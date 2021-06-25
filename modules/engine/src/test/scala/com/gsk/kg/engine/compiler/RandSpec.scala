package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType

import com.gsk.kg.engine.Compiler
import com.gsk.kg.engine.functions.Literals.NumericLiteral
import com.gsk.kg.engine.functions.Literals.isDoubleNumericLiteral
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class RandSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  /*
  https://www.w3.org/TR/sparql11-query/#idp2130040
  RAND() -> "0.31221030831984886"^^xsd:double
   */

  lazy val df: DataFrame = List(
    ("_:a", "<http://xmlns.com/foaf/0.1/name>", "Alice", ""),
    ("_:b", "<http://xmlns.com/foaf/0.1/name>", "Bob", ""),
    ("_:c", "<http://xmlns.com/foaf/0.1/name>", "Alice", "")
  ).toDF("s", "p", "o", "g")

  "perform rand function correctly" when {
    "select rand response with an RAND valid" in {

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT RAND()
          |WHERE  {
          |   ?x foaf:name ?name
          |}
          |""".stripMargin

      evaluate(df, query)
    }

    "bind rand response with an RAND valid" in {

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT ?r
          |WHERE  {
          |   ?x foaf:name ?name .
          |   bind(rand() as ?r)
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
    df.show(false)
    dfR.show(false)
    dfR
      .select(
        isDoubleNumericLiteral(col(dfR.columns.head)) &&
          NumericLiteral(col(dfR.columns.head)).value.cast(DoubleType).isNotNull
      )
      .collect()
      .toSet shouldEqual expected
  }

}
