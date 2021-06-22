package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ArithmeticSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "perform query with XPath Arithmetics" when {

    "add" should {

      "return expected value" in {

        val df: DataFrame = List(
          ("_:a", "<http://xmlns.org/foaf/0.1/xAxis>", "1.5"),
          ("_:a", "<http://xmlns.org/foaf/0.1/yAxis>", "2.5"),
          ("_:b", "<http://xmlns.org/foaf/0.1/xAxis>", "3.5"),
          ("_:b", "<http://xmlns.org/foaf/0.1/yAxis>", "4.5")
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
            |
            |SELECT ?s ?result
            |WHERE {
            | ?s foaf:xAxis ?x .
            | ?s foaf:yAxis ?y .
            | BIND((?x + ?y) AS ?result)
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config).right.get

        result.collect.length shouldEqual 2
        result.collect.toSet shouldEqual Set(
          Row("_:a", "4.0"),
          Row("_:b", "8.0")
        )
      }
    }

    "subtract" should {

      "return expected value" in {

        val df: DataFrame = List(
          ("_:a", "<http://xmlns.org/foaf/0.1/xAxis>", "1.5"),
          ("_:a", "<http://xmlns.org/foaf/0.1/yAxis>", "2.5"),
          ("_:b", "<http://xmlns.org/foaf/0.1/xAxis>", "3.5"),
          ("_:b", "<http://xmlns.org/foaf/0.1/yAxis>", "4.5")
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
            |
            |SELECT ?s ?result
            |WHERE {
            | ?s foaf:xAxis ?x .
            | ?s foaf:yAxis ?y .
            | BIND((?x - ?y) AS ?result)
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config).right.get

        result.collect.length shouldEqual 2
        result.collect.toSet shouldEqual Set(
          Row("_:a", "-1.0"),
          Row("_:b", "-1.0")
        )
      }
    }

    "multiply" should {

      "return expected value" in {

        val df: DataFrame = List(
          ("_:a", "<http://xmlns.org/foaf/0.1/xAxis>", "1.5"),
          ("_:a", "<http://xmlns.org/foaf/0.1/yAxis>", "2.5"),
          ("_:b", "<http://xmlns.org/foaf/0.1/xAxis>", "3.5"),
          ("_:b", "<http://xmlns.org/foaf/0.1/yAxis>", "4.5")
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
            |
            |SELECT ?s ?result
            |WHERE {
            | ?s foaf:xAxis ?x .
            | ?s foaf:yAxis ?y .
            | BIND((?x * ?y) AS ?result)
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config).right.get

        result.collect.length shouldEqual 2
        result.collect.toSet shouldEqual Set(
          Row("_:a", "3.75"),
          Row("_:b", "15.75")
        )
      }
    }

    "divide" should {

      "return expected value" in {

        val df: DataFrame = List(
          ("_:a", "<http://xmlns.org/foaf/0.1/xAxis>", "1.5"),
          ("_:a", "<http://xmlns.org/foaf/0.1/yAxis>", "2.5"),
          ("_:b", "<http://xmlns.org/foaf/0.1/xAxis>", "3.5"),
          ("_:b", "<http://xmlns.org/foaf/0.1/yAxis>", "4.5")
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
            |
            |SELECT ?s ?result
            |WHERE {
            | ?s foaf:xAxis ?x .
            | ?s foaf:yAxis ?y .
            | BIND((?x / ?y) AS ?result)
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config).right.get

        result.collect.length shouldEqual 2
        result.collect.toSet shouldEqual Set(
          Row("_:a", "0.6"),
          Row("_:b", "0.7777777777777778")
        )
      }
    }
  }
}
