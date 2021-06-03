package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SameTermSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "perform SAMETERM query" should {

    "execute and obtain expected results" when {

      "simple query" in {

        val df: DataFrame = List(
          ("_:a", "<http://xmlns.com/foaf/0.1/name>", "Alice"),
          (
            "_:a",
            "<http://xmlns.com/foaf/0.1/mbox>",
            "<mailto:alice@work.example>"
          ),
          ("_:b", "<http://xmlns.com/foaf/0.1/name>", "Ms A."),
          (
            "_:b",
            "<http://xmlns.com/foaf/0.1/mbox>",
            "<mailto:alice@work.example>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?name1 ?name2
            |WHERE { 
            | ?x foaf:name  ?name1 ;
            | foaf:mbox  ?mbox1 .
            | ?y foaf:name  ?name2 ;
            | foaf:mbox  ?mbox2 .
            | FILTER (sameTerm(?mbox1, ?mbox2) && !sameTerm(?name1, ?name2))
            |} 
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"Alice\"", "\"Ms A.\""),
          Row("\"Ms A.\"", "\"Alice\"")
        )
      }
    }
  }
}
