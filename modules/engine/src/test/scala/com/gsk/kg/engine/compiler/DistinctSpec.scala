package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DistinctSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "perform query with DISTINCT modifier" should {

    "execute and obtain expected results" in {

      val df: DataFrame = List(
        ("_:a", "<http://xmlns.com/foaf/0.1/name>", "Alice", ""),
        ("_:b", "<http://xmlns.com/foaf/0.1/name>", "Bob", ""),
        ("_:c", "<http://xmlns.com/foaf/0.1/name>", "Alice", "")
      ).toDF("s", "p", "o", "g")

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT DISTINCT ?name
          |WHERE  {
          |   ?x foaf:name  ?name
          |}
          |""".stripMargin

      val result = Compiler.compile(df, query, config)

      result shouldBe a[Right[_, _]]
      result.right.get.collect.length shouldEqual 2
      result.right.get.collect.toSet shouldEqual Set(
        Row("\"Alice\""),
        Row("\"Bob\"")
      )
    }
  }
}
