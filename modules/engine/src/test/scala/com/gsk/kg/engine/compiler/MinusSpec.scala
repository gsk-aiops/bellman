package com.gsk.kg.engine.compiler
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MinusSpec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
    with TestConfig {

  "Minus" should {

    "generate an empty dataset when minus-ing on three variables" in {

      import sqlContext.implicits._

      val df = List(
        ("a", "b", "c"),
        ("a", "b", "c"),
        ("a", "b", "c"),
        ("a", "b", "c"),
        ("a", "b", "c")
      ).toDF("s", "p", "o")

      val query = """
       SELECT *
       {
         { ?s ?p ?o }
         MINUS
         { ?x ?y ?z }
       }
       """

      val result = Compiler.compile(df, query, config) match {
        case Left(a)  => throw new RuntimeException(a.toString)
        case Right(b) => b
      }

      result.isEmpty shouldEqual true

    }

    "exclude triples in the second bgp from the result" in {

      import sqlContext.implicits._

      val df = List(
        ("<http://example.com/a>", "<http://example.com/b>", "c"),
        ("<http://example.com/d>", "<http://example.com/e>", "f"),
        ("<http://example.com/g>", "<http://example.com/h>", "i"),
        ("<http://example.com/j>", "<http://example.com/k>", "l"),
        ("<http://example.com/m>", "<http://example.com/n>", "o")
      ).toDF("s", "p", "o")

      val query = """
       SELECT *
       {
         { ?s  ?p  ?o }
         MINUS
         { ?s ?p ?o .
           filter( ?s = <http://example.com/m> && ?p = <http://example.com/n> && ?o = "o")
         }
       }
       """

      val result = Compiler.compile(df, query, config) match {
        case Left(a)  => throw new RuntimeException(a.toString)
        case Right(b) => b
      }

      result.collect.length shouldEqual 4
      result.collect.toSet shouldEqual Set(
        Row("<http://example.com/a>", "<http://example.com/b>", "\"c\""),
        Row("<http://example.com/d>", "<http://example.com/e>", "\"f\""),
        Row("<http://example.com/g>", "<http://example.com/h>", "\"i\""),
        Row("<http://example.com/j>", "<http://example.com/k>", "\"l\"")
      )
    }

  }

}
