package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class BlankNodeSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "perform query with Blank nodes" should {

    "execute and obtain expected results" in {

      val df: DataFrame = List(
        (
          "nodeA",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/predEntityClass>",
          "thisIsTheBlankNode",
          ""
        ),
        (
          "thisIsTheBlankNode",
          "<http://gsk-kg.rdip.gsk.com/dm/1.0/predClass>",
          "otherThingy",
          ""
        )
      ).toDF("s", "p", "o", "g")

      val query =
        """
          |PREFIX  dm:   <http://gsk-kg.rdip.gsk.com/dm/1.0/>
          |
          |SELECT ?de ?et
          |
          |WHERE {
          |  ?de dm:predEntityClass _:a .
          |  _:a dm:predClass ?et
          |}LIMIT 10
          |""".stripMargin

      val result = Compiler.compile(df, query, config)

      result shouldBe a[Right[_, _]]
      result.right.get.collect.toSet shouldEqual Set(
        Row("\"nodeA\"", "\"otherThingy\"")
      )
    }
  }
}
