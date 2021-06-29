package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class AbsSpec extends AnyWordSpec with Matchers with SparkSpec with TestConfig {

  import sqlContext.implicits._

  "perform abs function correctly" when {
    "abs a valid double numeric" in {

      val df: DataFrame = List(
        ("_:a", "<http://xmlns.com/foaf/0.1/num>", "1.65"),
        ("_:b", "<http://xmlns.com/foaf/0.1/num>", "1.71"),
        ("_:c", "<http://xmlns.com/foaf/0.1/num>", "-1.4")
      ).toDF("s", "p", "o")

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT ?r
          |WHERE  {
          |   ?x foaf:num ?num .
          |   bind(abs(?num) as ?r)
          |}
          |""".stripMargin

      val result = Compiler.compile(df, query, config)

      val dfR: DataFrame = result match {
        case Left(e)  => throw new Exception(e.toString)
        case Right(r) => r
      }
      val expected = List("1.65", "1.71", "1.4").map(Row(_))
      dfR
        .collect()
        .toList shouldEqual expected
    }

    "term is a simple number" in {
      val df = List(
        (
          "<http://uri.com/subject/#a1>",
          "<http://xmlns.com/foaf/0.1/num>",
          "-10.4"
        )
      ).toDF("s", "p", "o")

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT abs(?num)
          |WHERE  {
          |   ?x foaf:num ?num
          |}
          |""".stripMargin

      val expected = Row("10.4")
      val result   = Compiler.compile(df, query, config)
      val dfR: DataFrame = result match {
        case Left(e)  => throw new Exception(e.toString)
        case Right(r) => r
      }

      dfR
        .collect()
        .head shouldEqual expected
    }
  }

}
