package com.gsk.kg.engine.compiler

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.EngineError
import com.gsk.kg.sparqlparser.EngineError.ParsingError
import com.gsk.kg.sparqlparser.TestConfig
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class StrafterSpec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
    with TestConfig {

  import sqlContext.implicits._

  // This test should take into account argument compatibility
  // See: https://www.w3.org/TR/sparql11-query/#func-arg-compatibility
  "perform STRAFTER function correctly" should {

    "execute with no variables on parameters" when {

      "plain string and plain string" in {
        val df = List(
          (
            "Peter",
            "<http://xmlns.com/foaf/0.1/description>",
            "abc"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |SELECT ?desc
            |WHERE {
            | ?x foaf:description ?o .
            | BIND(STRAFTER("abc", "b") as ?desc) .
            |}
            |""".stripMargin

        val result =
          Compiler.compile(df, query, config)

        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"c\"")
        )
      }

      "language literal and plain string" in {
        val df = List(
          (
            "\"Peter\"",
            "<http://xmlns.com/foaf/0.1/description>",
            "\"abc\""
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |SELECT ?desc
            |WHERE {
            | ?x foaf:description ?o .
            | BIND(STRAFTER("abc"@en,"ab") as ?desc) .
            |}
            |""".stripMargin

        val result =
          Compiler.compile(df, query, config.copy(formatRdfOutput = true))

        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"c\"@en") // Jena's output
        )
      }

      // FIXME: This should return an engine error
      "language literal and language literal" in {
        val df = List(
          (
            "Peter",
            "<http://xmlns.com/foaf/0.1/description>",
            "abc"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |SELECT ?desc
            |WHERE {
            | ?x foaf:description ?o .
            | BIND(STRAFTER("abc"@en,"b"@cy) as ?desc) .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)
        result.left.get shouldBe a[EngineError]
      }

      "string literal and empty plain string" in {
        val df = List(
          (
            "Peter",
            "<http://xmlns.com/foaf/0.1/description>",
            "abc"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#string>
            |SELECT ?desc
            |WHERE {
            | ?x foaf:description ?o .
            | BIND(STRAFTER("abc"^^xsd:string,"") as ?desc) .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"\"") // Jena outputs empty string
        )
      }

      "language literal and no matching language literal" in {
        val df = List(
          (
            "Peter",
            "<http://xmlns.com/foaf/0.1/description>",
            "abc"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#string>
            |SELECT ?desc
            |WHERE {
            | ?x foaf:description ?o .
            | BIND(STRAFTER("abc"@en, "z"@en) as ?desc) .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"\"") // Jena outputs empty string
        )
      }

      "language literal and no matching plain string" in {
        val df = List(
          (
            "Peter",
            "<http://xmlns.com/foaf/0.1/description>",
            "abc"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#string>
            |SELECT ?desc
            |WHERE {
            | ?x foaf:description ?o .
            | BIND(STRAFTER("abc"@en, "z") as ?desc) .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"\"")
        )
      }

      // FIXME: It is returning a Parsing error
      "language literal and empty language literal" in {
        val df = List(
          (
            "Peter",
            "<http://xmlns.com/foaf/0.1/description>",
            "abc"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#string>
            |SELECT ?desc
            |WHERE {
            | ?x foaf:description ?o .
            | BIND(STRAFTER("abc"@en, ""@en) as ?desc) .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"abc\"@en")
        )
      }

      // FIXME: It is returning a parsing error
      "language literal and empty plain string" in {
        val df = List(
          (
            "Peter",
            "<http://xmlns.com/foaf/0.1/description>",
            "abc"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#string>
            |SELECT ?desc
            |WHERE {
            | ?x foaf:description ?o .
            | BIND(STRAFTER("abc"@en, "") as ?desc) .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"abc\"@en")
        )
      }

      "URI and string" in {
        val df = List(
          (
            "Peter",
            "<http://xmlns.com/foaf/0.1/description>",
            "abc"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#string>
            |SELECT ?desc
            |WHERE {
            | ?x foaf:description ?o .
            | BIND(STRAFTER(<http://example.org/abc>, "example.org") as ?desc) .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.left.get shouldBe a[ParsingError]
      }
    }

    "execute with variable on first parameter" when {

      "plain string variable and plain string" in {
        val df = List(
          (
            "Peter",
            "<http://xmlns.com/foaf/0.1/description>",
            "abc"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |SELECT ?desc
            |WHERE {
            | ?x foaf:description ?o .
            | BIND(STRAFTER(?o, "b") as ?desc) .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("\" an example\"")
        )
      }

      // FIXME: Trailing double quote
      "language literal variable and plain string" in {
        val df = List(
          (
            "Peter",
            "<http://xmlns.com/foaf/0.1/description>",
            "\"abc\"@en"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |SELECT ?desc
            |WHERE {
            | ?x foaf:description ?o .
            | BIND(STRAFTER(?s,"ab") as ?desc) .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"c\"^^@en") // Jena's output
        )
      }

      // FIXME: This should return an engine error
      "language literal variable and language literal" in {
        val df = List(
          (
            "Peter",
            "<http://xmlns.com/foaf/0.1/description>",
            "\"abc\"@en"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |SELECT ?desc
            |WHERE {
            | ?x foaf:description ?o .
            | BIND(STRAFTER(?s,"b"@cy) as ?desc) .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)
        result.left.get shouldBe a[EngineError]
      }

      // FIXME: Returns a parser error because of empty string
      "string literal variable and empty plain string" in {
        val df = List(
          (
            "Peter",
            "<http://xmlns.com/foaf/0.1/description>",
            "\"abc\"^^xsd:string"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#string>
            |SELECT ?desc
            |WHERE {
            | ?x foaf:description ?o .
            | BIND(STRAFTER(?s,"") as ?desc) .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("") // Jena outputs empty string
        )
      }

      // FIXME: It should return empty string
      "language literal variable and no matching language literal" in {
        val df = List(
          (
            "Peter",
            "<http://xmlns.com/foaf/0.1/description>",
            "\"abc\"@en"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#string>
            |SELECT ?desc
            |WHERE {
            | ?x foaf:description ?o .
            | BIND(STRAFTER(?s, "z"@en) as ?desc) .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("") // Jena outputs empty string
        )
      }

      // FIXME: It should return empty string
      "language literal variable and no matching plain string" in {
        val df = List(
          (
            "Peter",
            "<http://xmlns.com/foaf/0.1/description>",
            "\"abc\"@en"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#string>
            |SELECT ?desc
            |WHERE {
            | ?x foaf:description ?o .
            | BIND(STRAFTER(?s, "z") as ?desc) .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("") // Jena outputs empty string
        )
      }

      // FIXME: It is returning a Parsing error
      "language literal variable and empty language literal" in {
        val df = List(
          (
            "Peter",
            "<http://xmlns.com/foaf/0.1/description>",
            "\"abc\"@en"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#string>
            |SELECT ?desc
            |WHERE {
            | ?x foaf:description ?o .
            | BIND(STRAFTER(?s, ""@en) as ?desc) .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"abc\"@en")
        )
      }

      // FIXME: It is returning a parsing error
      "language literal variable and empty plain string" in {
        val df = List(
          (
            "Peter",
            "<http://xmlns.com/foaf/0.1/description>",
            "\"abc\"@en"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#string>
            |SELECT ?desc
            |WHERE {
            | ?x foaf:description ?o .
            | BIND(STRAFTER(?s, "") as ?desc) .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"abc\"@en")
        )
      }

      // FIXME: It should return an error as URIs should not be allowed in STRAFTER
      "URI variable and string" in {
        val df = List(
          (
            "Peter",
            "<http://xmlns.com/foaf/0.1/description>",
            "<http://example.org/abc>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#string>
            |SELECT ?desc
            |WHERE {
            | ?x foaf:description ?o .
            | BIND(STRAFTER(?s, "example.org") as ?desc) .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.left.get shouldBe a[ParsingError]
      }

      // FIXME: This returns a parser error
      "execute with string variable and string 2" in {
        val df = List(
          (
            "Peter",
            "<http://xmlns.com/foaf/0.1/description>",
            "abc def"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |SELECT ?desc
            |WHERE {
            | ?x foaf:description ?o .
            | BIND(STRAFTER(?o, " ") as ?desc) .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"def\"")
        )
      }
    }

    "execute with variable on second parameter" when {}

    "execute with variables on first and second parameters" when {}
  }
}
