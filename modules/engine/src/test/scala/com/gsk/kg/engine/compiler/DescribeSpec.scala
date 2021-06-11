package com.gsk.kg.engine
package compiler

import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import org.apache.spark.sql.Row

class DescribeSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "DESCRIBE" when {

    "describing a single value" should {

      "find all outgoing edges of the var" in {

        val df = List(
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Alice"
          ),
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/age>",
            "21"
          ),
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/knows>",
            "Bob"
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/thisnodedoesntappear>",
            "potato"
          )
        ).toDF("s", "p", "o")

        val query = """
          PREFIX : <http://example.org/>
          
          DESCRIBE :alice"""

        val result = Compiler.compile(df, query, config) match {
          case Right(r) => r
          case Left(err) => throw new Exception(err.toString)
        }

        result.collect.toSet shouldEqual Set(
          Row(
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/name>",
            "\"Alice\""
          ),
          Row(
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/age>",
            "21"
          ),
          Row(
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/knows>",
            "\"Bob\""
          )
        )
      }
    }
  }
}
