package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class AskSpec extends AnyWordSpec with Matchers with SparkSpec with TestConfig {

  import sqlContext.implicits._

  "perform ASK queries" should {

    "execute and obtain expected results" when {

      "simple query should return true" in {

        val df: DataFrame = List(
          ("_:a", "<http://xmlns.com/foaf/0.1/name>", "Alice"),
          (
            "_:a",
            "<http://xmlns.com/foaf/0.1/homepage>",
            "<http://work.example.org/alice/>"
          ),
          ("_:b", "<http://xmlns.com/foaf/0.1/name>", "Bob"),
          (
            "_:b",
            "<http://xmlns.com/foaf/0.1/mbox>",
            "<mailto:bob@work.example>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |ASK  { ?x foaf:name  "Alice" }
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("true")
        )
      }

      "simple query should return false" in {

        val df: DataFrame = List(
          ("_:a", "<http://xmlns.com/foaf/0.1/name>", "Alice"),
          (
            "_:a",
            "<http://xmlns.com/foaf/0.1/homepage>",
            "<http://work.example.org/alice/>"
          ),
          ("_:b", "<http://xmlns.com/foaf/0.1/name>", "Bob"),
          (
            "_:b",
            "<http://xmlns.com/foaf/0.1/mbox>",
            "<mailto:bob@work.example>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |ASK  { ?x foaf:name  "Alice" ;
            |          foaf:mbox  <mailto:alice@work.example> }
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("false")
        )
      }
    }
  }

}
