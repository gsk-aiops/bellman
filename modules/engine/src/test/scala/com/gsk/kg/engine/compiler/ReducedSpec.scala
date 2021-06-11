package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ReducedSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "perform query with REDUCED modifier" when {

    "simple query" in {

      val df: DataFrame = List(
        ("_:x", "<http://xmlns.com/foaf/0.1/name>", "Alice"),
        (
          "_:x",
          "<http://xmlns.com/foaf/0.1/mbox>",
          "<mailto:alice@example.com>"
        ),
        ("_:y", "<http://xmlns.com/foaf/0.1/name>", "Alice"),
        (
          "_:y",
          "<http://xmlns.com/foaf/0.1/mbox>",
          "<mailto:asmith@example.com>"
        ),
        ("_:z", "<http://xmlns.com/foaf/0.1/name>", "Alice"),
        (
          "_:z",
          "<http://xmlns.com/foaf/0.1/mbox>",
          "<mailto:alice.smith@example.com>"
        )
      ).toDF("s", "p", "o")

      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT REDUCED ?name
          |WHERE { ?x foaf:name ?name }
          |""".stripMargin

      val result = Compiler.compile(df, query, config).right.get

      result.collect.length shouldEqual 1
      result.collect.toSet shouldEqual Set(
        Row("\"Alice\"")
      )
    }
  }
}
