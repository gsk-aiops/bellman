package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class GroupBySpec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
    with TestConfig {

  import sqlContext.implicits._

  "perform query with GROUP BY" should {

    "operate correctly when only GROUP BY appears" in {

      val df = List(
        (
          "<http://uri.com/subject/a1>",
          "<http://uri.com/predicate>",
          "<http://uri.com/object>"
        ),
        (
          "<http://uri.com/subject/a1>",
          "<http://uri.com/predicate>",
          "<http://uri.com/object>"
        ),
        (
          "<http://uri.com/subject/a2>",
          "<http://uri.com/predicate>",
          "<http://uri.com/object>"
        ),
        (
          "<http://uri.com/subject/a2>",
          "<http://uri.com/predicate>",
          "<http://uri.com/object>"
        ),
        (
          "<http://uri.com/subject/a3>",
          "<http://uri.com/predicate>",
          "<http://uri.com/object>"
        )
      ).toDF("s", "p", "o")

      val query =
        """
          SELECT ?a
          WHERE {
            ?a <http://uri.com/predicate> <http://uri.com/object>
          } GROUP BY ?a
          """

      val result = Compiler.compile(df, query, config)

      result.right.get.collect.toSet shouldEqual Set(
        Row("<http://uri.com/subject/a1>"),
        Row("<http://uri.com/subject/a2>"),
        Row("<http://uri.com/subject/a3>")
      )
    }

    "operate correctly there's GROUP BY and a COUNT function" in {

      val df = List(
        (
          "<http://uri.com/subject/a1>",
          "<http://uri.com/predicate/p1>",
          "<http://uri.com/object/o1>"
        ),
        (
          "<http://uri.com/subject/a1>",
          "<http://uri.com/predicate/p2>",
          "<http://uri.com/object/o1>"
        ),
        (
          "<http://uri.com/subject/a2>",
          "<http://uri.com/predicate/p3>",
          "<http://uri.com/object/o1>"
        ),
        (
          "<http://uri.com/subject/a2>",
          "<http://uri.com/predicate/p4>",
          "<http://uri.com/object/o1>"
        ),
        (
          "<http://uri.com/subject/a3>",
          "<http://uri.com/predicate/p5>",
          "<http://uri.com/object/o1>"
        )
      ).toDF("s", "p", "o")

      val query =
        """
          |PREFIX obj: <http://uri.com/object/>
          |
          |SELECT ?a COUNT(?a)
          |WHERE {
          | ?a ?p obj:o1
          |} GROUP BY ?a
          |""".stripMargin

      val result = Compiler.compile(df, query, config)

      result.right.get.collect.toSet shouldEqual Set(
        Row("<http://uri.com/subject/a1>", "2"),
        Row("<http://uri.com/subject/a2>", "2"),
        Row("<http://uri.com/subject/a3>", "1")
      )
    }

    "operate correctly there's GROUP BY and a AVG function" in {

      val df = List(
        (
          "<http://uri.com/subject/a1>",
          "\"1\"^^xsd:int",
          "<http://uri.com/object>"
        ),
        (
          "<http://uri.com/subject/a1>",
          "\"2.0\"^^xsd:float",
          "<http://uri.com/object>"
        ),
        (
          "<http://uri.com/subject/a2>",
          "3",
          "<http://uri.com/object>"
        ),
        (
          "<http://uri.com/subject/a2>",
          "\"4\"^^xsd:decimal",
          "<http://uri.com/object>"
        ),
        (
          "<http://uri.com/subject/a3>",
          "\"5.0\"^^xsd:double",
          "<http://uri.com/object>"
        )
      ).toDF("s", "p", "o")

      val query =
        """
          SELECT ?a AVG(?b)
          WHERE {
            ?a ?b <http://uri.com/object>
          } GROUP BY ?a
          """

      val result = Compiler.compile(df, query, config)

      result.right.get.collect.toSet shouldEqual Set(
        Row("<http://uri.com/subject/a1>", "1.5"),
        Row("<http://uri.com/subject/a2>", "3.5"),
        Row("<http://uri.com/subject/a3>", "5.0")
      )
    }

    "operate correctly there's GROUP BY and a MIN function" when {

      "applied on strings" in {
        val df = List(
          (
            "<http://uri.com/subject/a1>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Alice"
          ),
          (
            "<http://uri.com/subject/a1>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Bob"
          ),
          (
            "<http://uri.com/subject/a2>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Charles"
          ),
          (
            "<http://uri.com/subject/a2>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Charlie"
          ),
          (
            "<http://uri.com/subject/a3>",
            "<http://xmlns.com/foaf/0.1/name>",
            "megan"
          ),
          (
            "<http://uri.com/subject/a3>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Megan"
          )
        ).toDF("s", "p", "o")

        val query =
          """
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>

          SELECT ?s (MIN(?name) AS ?o)
          WHERE {
            ?s foaf:name ?name .
          } GROUP BY ?s
          """

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set(
          Row("<http://uri.com/subject/a1>", "\"Alice\""),
          Row("<http://uri.com/subject/a2>", "\"Charles\""),
          Row("<http://uri.com/subject/a3>", "\"Megan\"")
        )
      }

      "applied on numbers" in {

        val df = List(
          (
            "<http://uri.com/subject/a1>",
            "<http://xmlns.com/foaf/0.1/age>",
            "18.1"
          ),
          (
            "<http://uri.com/subject/a1>",
            "<http://xmlns.com/foaf/0.1/age>",
            "19"
          ),
          (
            "<http://uri.com/subject/a2>",
            "<http://xmlns.com/foaf/0.1/age>",
            "30"
          ),
          (
            "<http://uri.com/subject/a2>",
            "<http://xmlns.com/foaf/0.1/age>",
            "31.5"
          ),
          (
            "<http://uri.com/subject/a3>",
            "<http://xmlns.com/foaf/0.1/age>",
            "45"
          ),
          (
            "<http://uri.com/subject/a3>",
            "<http://xmlns.com/foaf/0.1/age>",
            "50"
          )
        ).toDF("s", "p", "o")

        val query =
          """
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>

          SELECT ?s (MIN(?age) AS ?o)
          WHERE {
            ?s foaf:age ?age .
          } GROUP BY ?s
          """

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set(
          Row("<http://uri.com/subject/a1>", "18.1"),
          Row("<http://uri.com/subject/a2>", "30"),
          Row("<http://uri.com/subject/a3>", "45")
        )
      }
    }

    "operate correctly there's GROUP BY and a MAX function" when {

      "applied on strings" in {
        val df = List(
          (
            "<http://uri.com/subject/a1>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Alice"
          ),
          (
            "<http://uri.com/subject/a1>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Bob"
          ),
          (
            "<http://uri.com/subject/a2>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Charles"
          ),
          (
            "<http://uri.com/subject/a2>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Charlie"
          ),
          (
            "<http://uri.com/subject/a3>",
            "<http://xmlns.com/foaf/0.1/name>",
            "megan"
          ),
          (
            "<http://uri.com/subject/a3>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Megan"
          )
        ).toDF("s", "p", "o")

        val query =
          """
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>

          SELECT ?s (MAX(?name) AS ?o)
          WHERE {
            ?s foaf:name ?name .
          } GROUP BY ?s
          """

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set(
          Row("<http://uri.com/subject/a1>", "\"Bob\""),
          Row("<http://uri.com/subject/a2>", "\"Charlie\""),
          Row("<http://uri.com/subject/a3>", "\"megan\"")
        )
      }

      "applied on numbers" in {

        val df = List(
          (
            "<http://uri.com/subject/a1>",
            "<http://xmlns.com/foaf/0.1/age>",
            "18.1"
          ),
          (
            "<http://uri.com/subject/a1>",
            "<http://xmlns.com/foaf/0.1/age>",
            "19"
          ),
          (
            "<http://uri.com/subject/a2>",
            "<http://xmlns.com/foaf/0.1/age>",
            "30"
          ),
          (
            "<http://uri.com/subject/a2>",
            "<http://xmlns.com/foaf/0.1/age>",
            "31.5"
          ),
          (
            "<http://uri.com/subject/a3>",
            "<http://xmlns.com/foaf/0.1/age>",
            "45"
          ),
          (
            "<http://uri.com/subject/a3>",
            "<http://xmlns.com/foaf/0.1/age>",
            "50"
          )
        ).toDF("s", "p", "o")

        val query =
          """
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>

          SELECT ?s (MAX(?age) AS ?o)
          WHERE {
            ?s foaf:age ?age .
          } GROUP BY ?s
          """

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set(
          Row("<http://uri.com/subject/a1>", "19"),
          Row("<http://uri.com/subject/a2>", "31.5"),
          Row("<http://uri.com/subject/a3>", "50")
        )
      }
    }

    "not work for non RDF literal values" in {
      val df = List(
        ("<http://uri.com/subject/a1>", "hi", "<http://uri.com/object>")
      ).toDF("s", "p", "o")

      val query =
        """
          SELECT ?a SUM(?b)
          WHERE {
            ?a ?b <http://uri.com/object>
          } GROUP BY ?a
          """

      val result = Compiler.compile(df, query, config)

      result.right.get.collect.toSet shouldEqual Set(
        Row("<http://uri.com/subject/a1>", null)
      )
    }

    "operate correctly there's GROUP BY and a SUM function using typed literals" in {

      val df = List(
        (
          "<http://uri.com/subject/a1>",
          "2",
          "<http://uri.com/object>"
        ),
        (
          "<http://uri.com/subject/a1>",
          "\"1.0\"^^xsd:float",
          "<http://uri.com/object>"
        ),
        (
          "<http://uri.com/subject/a2>",
          "\"2\"^^xsd:decimal",
          "<http://uri.com/object>"
        ),
        (
          "<http://uri.com/subject/a2>",
          "\"1.0\"^^xsd:double",
          "<http://uri.com/object>"
        ),
        (
          "<http://uri.com/subject/a3>",
          "\"1\"^^xsd:numeric",
          "<http://uri.com/object>"
        )
      ).toDF("s", "p", "o")

      val query =
        """
          SELECT ?a SUM(?b)
          WHERE {
            ?a ?b <http://uri.com/object>
          } GROUP BY ?a
          """

      val result = Compiler.compile(df, query, config)

      result.right.get.collect.toSet shouldEqual Set(
        Row("<http://uri.com/subject/a1>", "3.0"),
        Row("<http://uri.com/subject/a2>", "3.0"),
        Row("<http://uri.com/subject/a3>", "1.0")
      )
    }

    "operate correctly there's GROUP BY and a AVG function using typed literals" in {

      val df = List(
        (
          "<http://uri.com/subject/a1>",
          "\"2\"^^xsd:int",
          "<http://uri.com/object>"
        ),
        (
          "<http://uri.com/subject/a1>",
          "\"1\"^^xsd:float",
          "<http://uri.com/object>"
        ),
        (
          "<http://uri.com/subject/a2>",
          "\"2.0\"^^xsd:float",
          "<http://uri.com/object>"
        ),
        (
          "<http://uri.com/subject/a2>",
          "1",
          "<http://uri.com/object>"
        ),
        (
          "<http://uri.com/subject/a3>",
          "\"1.5\"^^xsd:numeric",
          "<http://uri.com/object>"
        )
      ).toDF("s", "p", "o")

      val query =
        """
          SELECT ?a AVG(?b)
          WHERE {
            ?a ?b <http://uri.com/object>
          } GROUP BY ?a
          """

      val result = Compiler.compile(df, query, config)

      result.right.get.collect.toSet shouldEqual Set(
        Row("<http://uri.com/subject/a1>", "1.5"),
        Row("<http://uri.com/subject/a2>", "1.5"),
        Row("<http://uri.com/subject/a3>", "1.5")
      )
    }

    "operate correctly there's GROUP BY and a SAMPLE function" in {

      val df = List(
        (
          "<http://uri.com/subject/a1>",
          "<http://uri.com/predicate/1>",
          "<http://uri.com/object>"
        ),
        (
          "<http://uri.com/subject/a1>",
          "<http://uri.com/predicate/2>",
          "<http://uri.com/object>"
        ),
        (
          "<http://uri.com/subject/a2>",
          "<http://uri.com/predicate/3>",
          "<http://uri.com/object>"
        ),
        (
          "<http://uri.com/subject/a2>",
          "<http://uri.com/predicate/4>",
          "<http://uri.com/object>"
        ),
        (
          "<http://uri.com/subject/a3>",
          "<http://uri.com/predicate/5>",
          "<http://uri.com/object>"
        )
      ).toDF("s", "p", "o")

      val query =
        """
          SELECT ?a SAMPLE(?b)
          WHERE {
            ?a ?b <http://uri.com/object>
          } GROUP BY ?a
          """

      val result = Compiler.compile(df, query, config)

      result.right.get.collect should have length 3
    }

    "work correctly when the query doesn't contain an explicit GROUP BY clause" in {

      val df = List(
        (
          "<http://uri.com/subject/a1>",
          "<http://uri.com/predicate>",
          "<http://uri.com/object>"
        ),
        (
          "<http://uri.com/subject/a1>",
          "<http://uri.com/predicate>",
          "<http://uri.com/object>"
        ),
        (
          "<http://uri.com/subject/a2>",
          "<http://uri.com/predicate>",
          "<http://uri.com/object>"
        ),
        (
          "<http://uri.com/subject/a2>",
          "<http://uri.com/predicate>",
          "<http://uri.com/object>"
        ),
        (
          "<http://uri.com/subject/a3>",
          "<http://uri.com/predicate>",
          "<http://uri.com/object>"
        )
      ).toDF("s", "p", "o")

      val query =
        """
          SELECT COUNT(?a)
          WHERE {
            ?a <http://uri.com/predicate> <http://uri.com/object>
          }
          """

      val result = Compiler.compile(df, query, config)

      result.right.get.collect.toSet shouldEqual Set(
        Row("5")
      )
    }

    "work correctly in queries with more than one aggregator" in {

      val df = List(
        (
          "<http://uri.com/subject/a1>",
          "<http://uri.com/predicate>",
          "\"3\"^^xsd:int"
        ),
        (
          "<http://uri.com/subject/a1>",
          "<http://uri.com/predicate>",
          "\"3\"^^xsd:int"
        ),
        (
          "<http://uri.com/subject/a2>",
          "<http://uri.com/predicate>",
          "\"3\"^^xsd:int"
        ),
        (
          "<http://uri.com/subject/a2>",
          "<http://uri.com/predicate>",
          "\"3\"^^xsd:int"
        ),
        (
          "<http://uri.com/subject/a3>",
          "<http://uri.com/predicate>",
          "\"3\"^^xsd:int"
        )
      ).toDF("s", "p", "o")

      val query =
        """
          SELECT COUNT(?o) AVG(?o)
          WHERE {
            ?s ?p ?o
          }
          """

      val result = Compiler.compile(df, query, config) match {
        case Left(a)  => throw new RuntimeException(a.toString)
        case Right(b) => b
      }

      result.collect.toSet shouldEqual Set(
        Row("5")
      )
    }
  }
}
