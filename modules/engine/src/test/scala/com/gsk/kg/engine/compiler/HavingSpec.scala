package com.gsk.kg.engine.compiler

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class HavingSpec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
    with TestConfig {

  import sqlContext.implicits._

  "performs HAVING queries" should {

    "execute and obtain expected results" when {

      "single condition on HAVING clause" in {

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

      "multiple condition on HAVING clause" should {}
    }
  }
}
