package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.EngineError
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ReplaceSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "perform query with REPLACE function" should {

    "execute and obtain expected results without flags" in {

      val df: DataFrame = List(
        ("example", "<http://xmlns.com/foaf/0.1/lit>", "abcd", ""),
        ("example", "<http://xmlns.com/foaf/0.1/lit>", "abaB", ""),
        ("example", "<http://xmlns.com/foaf/0.1/lit>", "bbBB", ""),
        ("example", "<http://xmlns.com/foaf/0.1/lit>", "aaaa", "")
      ).toDF("s", "p", "o", "g")

      val query =
        """
          |PREFIX foaf:   <http://xmlns.com/foaf/0.1/>
          |
          |SELECT   ?lit ?lit2
          |WHERE    {
          |  ?x foaf:lit ?lit .
          |  BIND(REPLACE(?lit, "b", "Z") AS ?lit2)
          |}
          |""".stripMargin

      val result = Compiler.compile(df, query, config)

      result shouldBe a[Right[_, _]]
      result.right.get.collect.length shouldEqual 4
      result.right.get.collect.toSet shouldEqual Set(
        Row("\"abcd\"", "\"aZcd\""),
        Row("\"abaB\"", "\"aZaB\""),
        Row("\"bbBB\"", "\"ZZBB\""),
        Row("\"aaaa\"", "\"aaaa\"")
      )
    }

    "execute and obtain expected results with flags" in {

      val df: DataFrame = List(
        ("example", "<http://xmlns.com/foaf/0.1/lit>", "abcd", ""),
        ("example", "<http://xmlns.com/foaf/0.1/lit>", "abaB", ""),
        ("example", "<http://xmlns.com/foaf/0.1/lit>", "bbBB", ""),
        ("example", "<http://xmlns.com/foaf/0.1/lit>", "aaaa", "")
      ).toDF("s", "p", "o", "g")

      val query =
        """
          |PREFIX foaf:   <http://xmlns.com/foaf/0.1/>
          |
          |SELECT   ?lit ?lit2
          |WHERE    {
          |  ?x foaf:lit ?lit .
          |  BIND(REPLACE(?lit, "b", "Z", "i") AS ?lit2)
          |}
          |""".stripMargin

      val result = Compiler.compile(df, query, config)

      result shouldBe a[Right[_, _]]
      result.right.get.collect.length shouldEqual 4
      result.right.get.collect.toSet shouldEqual Set(
        Row("\"abcd\"", "\"aZcd\""),
        Row("\"abaB\"", "\"aZaZ\""),
        Row("\"bbBB\"", "\"ZZZZ\""),
        Row("\"aaaa\"", "\"aaaa\"")
      )
    }

    // TODO: Capture Spark exceptions on the Left projection once the query trigger the Spark action
    // this can be done probably at the Engine level
    "execute and obtain an expected error, " +
      "because the pattern matches the zero-length string" ignore {

        val df: DataFrame = List(
          ("example", "<http://xmlns.com/foaf/0.1/lit>", "abracadabra", "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
          |PREFIX foaf:   <http://xmlns.com/foaf/0.1/>
          |
          |SELECT   ?lit ?lit2
          |WHERE    {
          |  ?x foaf:lit ?lit .
          |  BIND(REPLACE(?lit, ".*?", "$1") AS ?lit2)
          |}
          |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Left[_, _]]
        result.left.get shouldEqual EngineError.FunctionError(
          s"Error on REPLACE function: No group 1"
        )
      }
  }
}
