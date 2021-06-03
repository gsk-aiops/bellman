package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class HavingSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "performs HAVING queries" should {

    "execute and obtain expected results" when {

      "single condition on HAVING clause with COUNT" in {

        val df: DataFrame = List(
          ("_:a", "<http://example.org/size>", "5"),
          ("_:a", "<http://example.org/size>", "15"),
          ("_:a", "<http://example.org/size>", "20"),
          ("_:a", "<http://example.org/size>", "7"),
          ("_:b", "<http://example.org/size>", "9.5"),
          ("_:b", "<http://example.org/size>", "1"),
          ("_:b", "<http://example.org/size>", "11")
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX ex: <http://example.org/>
            |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            |
            |SELECT ?x (COUNT(?size) AS ?count)
            |WHERE {
            |  ?x ex:size ?size
            |}
            |GROUP BY ?x
            |HAVING(COUNT(?size) = 4)
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:a", "4")
        )
      }

      "single condition on HAVING clause with AVG" in {

        val df: DataFrame = List(
          ("_:a", "<http://example.org/size>", "5"),
          ("_:a", "<http://example.org/size>", "15"),
          ("_:a", "<http://example.org/size>", "20"),
          ("_:a", "<http://example.org/size>", "7"),
          ("_:b", "<http://example.org/size>", "9.5"),
          ("_:b", "<http://example.org/size>", "1"),
          ("_:b", "<http://example.org/size>", "11"),
          ("_:b", "<http://example.org/size>", "8")
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX ex: <http://example.org/>
            |
            |SELECT (AVG(?size) AS ?asize)
            |WHERE {
            |  ?x ex:size ?size
            |}
            |GROUP BY ?x
            |HAVING(AVG(?size) > 10)
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(Row("11.75"))
      }
    }
  }
}
