package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class OffsetSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "perform query with OFFSET modifier" should {

    "execute with offset greater than 0 and obtain a non empty set" in {

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
          |OFFSET 1
          |""".stripMargin

      val result = Compiler.compile(df, query, config)

      result shouldBe a[Right[_, _]]
      result.right.get.collect.length shouldEqual 2
      result.right.get.collect.toSet shouldEqual Set(
        Row("\"Perico\""),
        Row("\"Henry\"")
      )
    }

    "execute with offset equal to 0 and obtain same elements as the original set" in {

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
          |OFFSET 0
          |""".stripMargin

      val result = Compiler.compile(df, query, config)

      result shouldBe a[Right[_, _]]
      result.right.get.collect.length shouldEqual 3
      result.right.get.collect.toSet shouldEqual Set(
        Row("\"Anthony\""),
        Row("\"Perico\""),
        Row("\"Henry\"")
      )
    }

    "execute with offset greater than the number of elements of the dataframe and obtain an empty set" in {

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
          |OFFSET 5
          |""".stripMargin

      val result = Compiler.compile(df, query, config)

      result shouldBe a[Right[_, _]]
      result.right.get.collect.length shouldEqual 0
      result.right.get.collect.toSet shouldEqual Set.empty
    }
  }
}
