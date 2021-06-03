package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.EngineError
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class LimitSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "perform query with LIMIT modifier" should {

    "execute with limit greater than 0" in {

      val df: DataFrame = List(
        ("a", "b", "c", ""),
        ("team", "<http://xmlns.com/foaf/0.1/name>", "Anthony", ""),
        ("team", "<http://xmlns.com/foaf/0.1/name>", "Perico", ""),
        ("team", "<http://xmlns.com/foaf/0.1/name>", "Henry", "")
      ).toDF("s", "p", "o", "g")

      val query =
        """
          |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
          |
          |SELECT  ?name
          |WHERE   { ?x foaf:name ?name }
          |LIMIT   2
          |""".stripMargin

      val result = Compiler.compile(df, query, config)

      result shouldBe a[Right[_, _]]
      result.right.get.collect.length shouldEqual 2
      result.right.get.collect.toSet shouldEqual Set(
        Row("\"Anthony\""),
        Row("\"Perico\"")
      )
    }

    "execute with limit equal to 0 and obtain no results" in {

      val df: DataFrame = List(
        ("a", "b", "c", ""),
        ("team", "<http://xmlns.com/foaf/0.1/name>", "Anthony", ""),
        ("team", "<http://xmlns.com/foaf/0.1/name>", "Perico", ""),
        ("team", "<http://xmlns.com/foaf/0.1/name>", "Henry", "")
      ).toDF("s", "p", "o", "g")

      val query =
        """
          |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
          |
          |SELECT  ?name
          |WHERE   { ?x foaf:name ?name }
          |LIMIT   0
          |""".stripMargin

      val result = Compiler.compile(df, query, config)

      result shouldBe a[Right[_, _]]
      result.right.get.collect.length shouldEqual 0
      result.right.get.collect.toSet shouldEqual Set.empty
    }

    "execute with limit greater than Java MAX INTEGER and obtain an error" in {

      val df: DataFrame = List(
        ("a", "b", "c", ""),
        ("team", "<http://xmlns.com/foaf/0.1/name>", "Anthony", ""),
        ("team", "<http://xmlns.com/foaf/0.1/name>", "Perico", ""),
        ("team", "<http://xmlns.com/foaf/0.1/name>", "Henry", "")
      ).toDF("s", "p", "o", "g")

      val query =
        """
          |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
          |
          |SELECT  ?name
          |WHERE   { ?x foaf:name ?name }
          |LIMIT   2147483648
          |""".stripMargin

      val result = Compiler.compile(df, query, config)

      result shouldBe a[Left[_, _]]
      result.left.get shouldEqual EngineError.NumericTypesDoNotMatch(
        "2147483648 to big to be converted to an Int"
      )
    }
  }
}
