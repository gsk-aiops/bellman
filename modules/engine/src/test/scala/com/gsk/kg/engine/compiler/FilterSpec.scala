package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class FilterSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "perform query with FILTER modifier" when {

    "single condition" should {

      "execute and obtain expected results" in {

        val df: DataFrame = List(
          ("a", "b", "c", ""),
          ("team", "<http://xmlns.com/foaf/0.1/name>", "Anthony", ""),
          ("team", "<http://xmlns.com/foaf/0.1/name>", "Perico", ""),
          ("team", "<http://xmlns.com/foaf/0.1/name>", "Henry", ""),
          ("_:", "<http://xmlns.com/foaf/0.1/name>", "Blank", "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT  ?name
            |WHERE   {
            |   ?x foaf:name ?name .
            |   FILTER isBlank(?x)
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(Row("\"Blank\""))
      }

      "execute and obtain expected results when condition has embedded functions" in {

        val df: DataFrame = List(
          ("a", "b", "c", ""),
          ("team", "<http://xmlns.com/foaf/0.1/name>", "Anthony", ""),
          ("team", "<http://xmlns.com/foaf/0.1/name>", "Perico", ""),
          ("team", "<http://xmlns.com/foaf/0.1/name>", "Henry", ""),
          ("a:", "<http://xmlns.com/foaf/0.1/name>", "Blank", "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT  ?name
            |WHERE   {
            |   ?x foaf:name ?name .
            |   FILTER (isBlank( replace (?x, "a", "_") ) )
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(Row("\"Blank\""))
      }

      "execute and obtain expected results when double filter" in {

        val df: DataFrame = List(
          ("a", "b", "c", ""),
          ("team", "<http://xmlns.com/foaf/0.1/name>", "Anthony", ""),
          ("_:a", "<http://xmlns.com/foaf/0.1/name>", "_:b", ""),
          ("foaf:c", "<http://xmlns.com/foaf/0.1/name>", "_:d", ""),
          ("_:e", "<http://xmlns.com/foaf/0.1/name>", "foaf:f", "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT  ?x ?name
            |WHERE   {
            |   ?x foaf:name ?name .
            |   FILTER isBlank(?x)
            |   FILTER isBlank(?name)
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:a", "_:b")
        )
      }

      "execute and obtain expected results when filter over all select statement" in {

        val df: DataFrame = List(
          (
            "<http://example.org/Network>",
            "<http://www.w3.org/2000/01/rdf-schema#subClassOf>",
            "<http://example.org/Main>",
            ""
          ),
          (
            "<http://example.org/ATM>",
            "<http://www.w3.org/2000/01/rdf-schema#subClassOf>",
            "<http://example.org/Network>",
            ""
          ),
          (
            "<http://example.org/ARPANET>",
            "<http://www.w3.org/2000/01/rdf-schema#subClassOf>",
            "<http://example.org/Network>",
            ""
          ),
          (
            "<http://example.org/Software>",
            "<http://www.w3.org/2000/01/rdf-schema#subClassOf>",
            "<http://example.org/Main>",
            ""
          ),
          (
            "_:Linux",
            "<http://www.w3.org/2000/01/rdf-schema#subClassOf>",
            "<http://example.org/Software>",
            ""
          ),
          (
            "<http://example.org/Windows>",
            "<http://www.w3.org/2000/01/rdf-schema#subClassOf>",
            "<http://example.org/Software>",
            ""
          ),
          (
            "<http://example.org/XP>",
            "<http://www.w3.org/2000/01/rdf-schema#subClassOf>",
            "<http://example.org/Windows>",
            ""
          ),
          (
            "<http://example.org/Win7>",
            "<http://www.w3.org/2000/01/rdf-schema#subClassOf>",
            "<http://example.org/Windows>",
            ""
          ),
          (
            "<http://example.org/Win8>",
            "<http://www.w3.org/2000/01/rdf-schema#subClassOf>",
            "<http://example.org/Windows>",
            ""
          ),
          (
            "<http://example.org/Ubuntu20>",
            "<http://www.w3.org/2000/01/rdf-schema#subClassOf>",
            "_:Linux",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX : <http://example.org/>
            |PREFIX rdf: <http://www.w3.org/2000/01/rdf-schema#>
            |
            |SELECT ?parent
            |WHERE {
            |   :Win8 rdf:subClassOf ?parent .
            |   FILTER (!isBlank(?parent))
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("<http://example.org/Windows>")
        )
      }

      // TODO: Un-ignore when implemented EQUALS and GT
      "execute and obtain expected results when complex filter" ignore {

        val df: DataFrame = List(
          ("_:a", "<http://xmlns.com/foaf/0.1/name>", "Alice", ""),
          (
            "_:a",
            "<http://example.org/stats#hits>",
            "\"2349\"^^xsd:integer",
            ""
          ),
          ("_:b", "<http://xmlns.com/foaf/0.1/name>", "Bob", ""),
          (
            "_:b",
            "<http://example.org/stats#hits>",
            "\"105\"^^xsd:integer",
            ""
          ),
          ("_:c", "<http://xmlns.com/foaf/0.1/name>", "Eve"),
          ("_:c", "<http://example.org/stats#hits>", "\"181\"^^xsd:integer", "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX site: <http://example.org/stats#>
            |
            |CONSTRUCT
            |{
            |   ?x foaf:name ?name .
            |   ?y site:hits ?hits
            |}
            |WHERE
            |{
            |   {
            |     ?x foaf:name ?name .
            |     FILTER (?name = "Bob")
            |   }
            |   UNION
            |   {
            |     ?y site:hits ?hits
            |     FILTER (?hits > 1000)
            |   }
            |   FILTER (isBlank(?x) || isBlank(?y))
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:a", "foaf:name", "\"Bob\"", ""),
          Row("_:b", "site:hits", "\"2349\"^^xsd:integer", "")
        )
      }
    }

    "multiple conditions" should {

      // TODO: Un-ignore when binary logical operations implemented
      "execute and obtain expected results when multiple conditions" ignore {

        val df: DataFrame = List(
          ("a", "b", "c", ""),
          ("team", "<http://xmlns.com/foaf/0.1/name>", "Anthony", ""),
          ("team", "<http://xmlns.com/foaf/0.1/name>", "Perico", ""),
          ("team", "<http://xmlns.com/foaf/0.1/name>", "Henry", ""),
          ("_:", "<http://xmlns.com/foaf/0.1/name>", "Blank", ""),
          (
            "_:",
            "<http://xmlns.com/foaf/0.1/name>",
            "<http://test-uri/blank>",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT  ?name
            |WHERE   {
            |   ?x foaf:name ?name .
            |   FILTER(isBlank(?x) && isURI(?x))
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(Row("\"Blank\""))
      }

      "execute and obtain expected results when there is AND condition" in {

        val df: DataFrame = List(
          ("team", "<http://xmlns.com/foaf/0.1/name>", "_:", ""),
          ("team", "<http://xmlns.com/foaf/0.1/name>", "Perico", ""),
          ("_:", "<http://xmlns.com/foaf/0.1/name>", "_:", ""),
          ("_:", "<http://xmlns.com/foaf/0.1/name>", "Henry", "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT  ?x ?name
            |WHERE   {
            |   ?x foaf:name ?name .
            |   FILTER(isBlank(?x) && !isBlank(?name))
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:", "\"Henry\"")
        )
      }

      "execute and obtain expected results when there is OR condition" in {

        val df: DataFrame = List(
          ("team", "<http://xmlns.com/foaf/0.1/name>", "_:", ""),
          ("team", "<http://xmlns.com/foaf/0.1/name>", "Perico", ""),
          ("_:", "<http://xmlns.com/foaf/0.1/name>", "_:", ""),
          ("_:", "<http://xmlns.com/foaf/0.1/name>", "Henry", "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT  ?x ?name
            |WHERE   {
            |   ?x foaf:name ?name .
            |   FILTER(isBlank(?x) || isBlank(?name))
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 3
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"team\"", "_:"),
          Row("_:", "_:"),
          Row("_:", "\"Henry\"")
        )
      }
    }

    "logical operation EQUALS" should {

      "execute on simple literal" in {

        val df: DataFrame = List(
          ("_:a", "<http://xmlns.com/foaf/0.1/name>", "Henry", ""),
          ("_:b", "<http://xmlns.com/foaf/0.1/name>", "Perico", "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?x ?name
            |WHERE   {
            |   ?x foaf:name ?name .
            |   FILTER(?name = "Henry")
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:a", "\"Henry\"")
        )
      }

      // TODO: Add support for string syntactic sugar, see: https://lists.w3.org/Archives/Public/public-sparql-dev/2013AprJun/0003.html
      "execute on strings" ignore {

        val df: DataFrame = List(
          (
            "_:a",
            "<http://xmlns.com/foaf/0.1/name>",
            "\"Henry\"^^<http://www.w3.org/2001/XMLSchema#string>",
            ""
          ),
          (
            "_:b",
            "<http://xmlns.com/foaf/0.1/name>",
            "\"Perico\"^^<http://www.w3.org/2001/XMLSchema#string>",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            |
            |SELECT ?x ?name
            |WHERE   {
            |   ?x foaf:name ?name .
            |   FILTER(?name = "Henry"^^xsd:string)
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row(":_a", "\"Henry\"^^xsd:string")
        )
      }

      "execute on numbers" in {

        val df: DataFrame = List(
          ("_:Perico", "<http://xmlns.com/foaf/0.1/age>", 15, ""),
          ("_:Henry", "<http://xmlns.com/foaf/0.1/age>", 21, "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?x ?age
            |WHERE   {
            |   ?x foaf:age ?age .
            |   FILTER(?age = 21)
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:Henry", "21")
        )
      }

      "execute on booleans" in {

        val df: DataFrame = List(
          ("_:Martha", "<http://xmlns.com/foaf/0.1/isFemale>", true, ""),
          ("_:Henry", "<http://xmlns.com/foaf/0.1/isFemale>", false, "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?x ?isFemale
            |WHERE   {
            |   ?x foaf:isFemale ?isFemale .
            |   FILTER(?isFemale = true)
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:Martha", "true")
        )
      }

      "execute on datetimes" in {

        val df: DataFrame = List(
          ("_:Martha", "<http://xmlns.com/foaf/0.1/isFemale>", "true", ""),
          ("_:Henry", "<http://xmlns.com/foaf/0.1/isFemale>", "false", ""),
          ("_:Ana", "<http://xmlns.com/foaf/0.1/isFemale>", "true", ""),
          (
            "_:Martha",
            "<http://xmlns.com/foaf/0.1/birthDay>",
            """"2000-10-10T10:10:10.000"^^xsd:dateTime""",
            ""
          ),
          (
            "_:Ana",
            "<http://xmlns.com/foaf/0.1/birthDay>",
            """"2000-10-10T10:10:10.000"^^xsd:dateTime""",
            ""
          ),
          (
            "_:Henry",
            "<http://xmlns.com/foaf/0.1/birthDay>",
            """"1990-10-10T10:10:10.000"^^xsd:dateTime""",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
            |
            |SELECT ?x
            |WHERE   {
            |   ?x foaf:birthDay ?bday .
            |   FILTER(?bday = "2000-10-10T10:10:10.000"^^xsd:dateTime)
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect should have length 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:Martha"),
          Row("_:Ana")
        )
      }
    }

    "logical operation NOT EQUALS" should {

      "execute on simple literal" in {

        val df: DataFrame = List(
          ("_:a", "<http://xmlns.com/foaf/0.1/name>", "Henry", ""),
          ("_:b", "<http://xmlns.com/foaf/0.1/name>", "Perico", "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?x ?name
            |WHERE   {
            |   ?x foaf:name ?name .
            |   FILTER(?name != "Henry")
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:b", "\"Perico\"")
        )
      }

      // TODO: Add support for string syntactic sugar, see: https://lists.w3.org/Archives/Public/public-sparql-dev/2013AprJun/0003.html
      "execute on strings" ignore {

        val df: DataFrame = List(
          (
            "_:a",
            "<http://xmlns.com/foaf/0.1/name>",
            "\"Henry\"^^<http://www.w3.org/2001/XMLSchema#string>",
            ""
          ),
          (
            "_:b",
            "<http://xmlns.com/foaf/0.1/name>",
            "\"Perico\"^^<http://www.w3.org/2001/XMLSchema#string>",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            |
            |SELECT ?x ?name
            |WHERE   {
            |   ?x foaf:name ?name .
            |   FILTER(?name != "Henry"^^xsd:string)
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row(":_b", "\"Perico\"^^xsd:string")
        )
      }

      "execute on numbers" in {

        val df: DataFrame = List(
          ("_:Perico", "<http://xmlns.com/foaf/0.1/age>", 15, ""),
          ("_:Henry", "<http://xmlns.com/foaf/0.1/age>", 21, "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?x ?age
            |WHERE   {
            |   ?x foaf:age ?age .
            |   FILTER(?age != 21)
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:Perico", "15")
        )
      }

      "execute on booleans" in {

        val df: DataFrame = List(
          ("_:Martha", "<http://xmlns.com/foaf/0.1/isFemale>", true, ""),
          ("_:Henry", "<http://xmlns.com/foaf/0.1/isFemale>", false, "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?x ?isFemale
            |WHERE   {
            |   ?x foaf:isFemale ?isFemale .
            |   FILTER(?isFemale != true)
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:Henry", "false")
        )
      }

      "execute on dateTimes" in {

        val df: DataFrame = List(
          ("_:Martha", "<http://xmlns.com/foaf/0.1/isFemale>", "true", ""),
          ("_:Henry", "<http://xmlns.com/foaf/0.1/isFemale>", "false", ""),
          ("_:Ana", "<http://xmlns.com/foaf/0.1/isFemale>", "true", ""),
          (
            "_:Martha",
            "<http://xmlns.com/foaf/0.1/birthDay>",
            """"2000-10-10T10:10:10.000"^^xsd:dateTime""",
            ""
          ),
          (
            "_:Ana",
            "<http://xmlns.com/foaf/0.1/birthDay>",
            """"2000-10-10T10:10:10.000"^^xsd:dateTime""",
            ""
          ),
          (
            "_:Henry",
            "<http://xmlns.com/foaf/0.1/birthDay>",
            """"1990-10-10T10:10:10.000"^^xsd:dateTime""",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
            |
            |SELECT ?x
            |WHERE   {
            |   ?x foaf:birthDay ?bday .
            |   FILTER(?bday != "2000-10-10T10:10:10.000"^^xsd:dateTime)
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect should have length 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:Henry")
        )
      }
    }

    "logical operation GT" should {

      "execute on simple literal" in {

        val df: DataFrame = List(
          ("_:a", "<http://xmlns.com/foaf/0.1/name>", "Anthony", ""),
          ("_:b", "<http://xmlns.com/foaf/0.1/name>", "Charles", "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?x ?name
            |WHERE   {
            |   ?x foaf:name ?name .
            |   FILTER(?name > "Bob")
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:b", "\"Charles\"")
        )
      }

      // TODO: Add support for string syntactic sugar, see: https://lists.w3.org/Archives/Public/public-sparql-dev/2013AprJun/0003.html
      "execute on strings" ignore {

        val df: DataFrame = List(
          (
            "_:a",
            "<http://xmlns.com/foaf/0.1/name>",
            "\"Anthony\"^^<http://www.w3.org/2001/XMLSchema#string>"
          ),
          (
            "_:b",
            "<http://xmlns.com/foaf/0.1/name>",
            "\"Charles\"^^<http://www.w3.org/2001/XMLSchema#string>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            |
            |SELECT ?x ?name
            |WHERE   {
            |   ?x foaf:name ?name .
            |   FILTER(?name > "Bob"^^xsd:string)
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:b", "\"Charles\"^^xsd:string")
        )
      }

      "execute on numbers" in {

        val df: DataFrame = List(
          ("_:Bob", "<http://xmlns.com/foaf/0.1/age>", 15, ""),
          ("_:Alice", "<http://xmlns.com/foaf/0.1/age>", 18, ""),
          ("_:Henry", "<http://xmlns.com/foaf/0.1/age>", 21, "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?x ?age
            |WHERE   {
            |   ?x foaf:age ?age .
            |   FILTER(?age > 18)
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:Henry", "21")
        )
      }

      "execute on booleans" in {

        val df: DataFrame = List(
          ("_:Martha", "<http://xmlns.com/foaf/0.1/isFemale>", true, ""),
          ("_:Henry", "<http://xmlns.com/foaf/0.1/isFemale>", false, "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?x ?isFemale
            |WHERE   {
            |   ?x foaf:isFemale ?isFemale .
            |   FILTER(?isFemale > true)
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 0
        result.right.get.collect.toSet shouldEqual Set()
      }

      // TODO: Implement Date Time support issue
      "execute on dateTimes" in {

        val df: DataFrame = List(
          ("_:Martha", "<http://xmlns.com/foaf/0.1/isFemale>", "true", ""),
          ("_:Henry", "<http://xmlns.com/foaf/0.1/isFemale>", "false", ""),
          ("_:Ana", "<http://xmlns.com/foaf/0.1/isFemale>", "true", ""),
          (
            "_:Martha",
            "<http://xmlns.com/foaf/0.1/birthDay>",
            """"2000-10-10T10:10:10.000"^^xsd:dateTime""",
            ""
          ),
          (
            "_:Ana",
            "<http://xmlns.com/foaf/0.1/birthDay>",
            """"2000-10-10T10:10:10.000"^^xsd:dateTime""",
            ""
          ),
          (
            "_:Henry",
            "<http://xmlns.com/foaf/0.1/birthDay>",
            """"1990-10-10T10:10:10.000"^^xsd:dateTime""",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
            |
            |SELECT ?x
            |WHERE   {
            |   ?x foaf:birthDay ?bday .
            |   FILTER(?bday > "1990-10-10T10:10:10.000"^^xsd:dateTime)
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect should have length 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:Martha"),
          Row("_:Ana")
        )
      }
    }

    "logical operation LT" should {

      "execute on simple literal" in {

        val df: DataFrame = List(
          ("_:a", "<http://xmlns.com/foaf/0.1/name>", "Anthony", ""),
          ("_:b", "<http://xmlns.com/foaf/0.1/name>", "Charles", "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?x ?name
            |WHERE   {
            |   ?x foaf:name ?name .
            |   FILTER(?name < "Bob")
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:a", "\"Anthony\"")
        )
      }

      // TODO: Add support for string syntactic sugar, see: https://lists.w3.org/Archives/Public/public-sparql-dev/2013AprJun/0003.html
      "execute on strings" ignore {

        val df: DataFrame = List(
          (
            "_:a",
            "<http://xmlns.com/foaf/0.1/name>",
            "\"Anthony\"^^<http://www.w3.org/2001/XMLSchema#string>",
            ""
          ),
          (
            "_:b",
            "<http://xmlns.com/foaf/0.1/name>",
            "\"Charles\"^^<http://www.w3.org/2001/XMLSchema#string>",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            |
            |SELECT ?x ?name
            |WHERE   {
            |   ?x foaf:name ?name .
            |   FILTER(?name < "Bob"^^xsd:string)
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:a", "\"Anthony\"^^xsd:string")
        )
      }

      "execute on numbers" in {

        val df: DataFrame = List(
          ("_:Bob", "<http://xmlns.com/foaf/0.1/age>", 15, ""),
          ("_:Alice", "<http://xmlns.com/foaf/0.1/age>", 18, ""),
          ("_:Henry", "<http://xmlns.com/foaf/0.1/age>", 21, "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?x ?age
            |WHERE   {
            |   ?x foaf:age ?age .
            |   FILTER(?age < 18)
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:Bob", "15")
        )
      }

      "execute on booleans" in {

        val df: DataFrame = List(
          ("_:Martha", "<http://xmlns.com/foaf/0.1/isFemale>", true, ""),
          ("_:Henry", "<http://xmlns.com/foaf/0.1/isFemale>", false, "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?x ?isFemale
            |WHERE   {
            |   ?x foaf:isFemale ?isFemale .
            |   FILTER(?isFemale < true)
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:Henry", "false")
        )
      }

      // TODO: Implement Date Time support issue
      "execute on dateTimes" in {

        val df: DataFrame = List(
          ("_:Martha", "<http://xmlns.com/foaf/0.1/isFemale>", "true", ""),
          ("_:Henry", "<http://xmlns.com/foaf/0.1/isFemale>", "false", ""),
          ("_:Ana", "<http://xmlns.com/foaf/0.1/isFemale>", "true", ""),
          (
            "_:Martha",
            "<http://xmlns.com/foaf/0.1/birthDay>",
            """"2000-10-10T10:10:10.000"^^xsd:dateTime""",
            ""
          ),
          (
            "_:Ana",
            "<http://xmlns.com/foaf/0.1/birthDay>",
            """"2000-10-10T10:10:10.000"^^xsd:dateTime""",
            ""
          ),
          (
            "_:Henry",
            "<http://xmlns.com/foaf/0.1/birthDay>",
            """"1990-10-10T10:10:10.000"^^xsd:dateTime""",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
            |
            |SELECT ?x
            |WHERE   {
            |   ?x foaf:birthDay ?bday .
            |   FILTER(?bday > "1990-10-10T10:10:10.000"^^xsd:dateTime)
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect should have length 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:Martha"),
          Row("_:Ana")
        )

      }
    }

    "logical operation GTE" should {

      "execute on simple literal" in {

        val df: DataFrame = List(
          ("_:a", "<http://xmlns.com/foaf/0.1/name>", "Anthony", ""),
          ("_:b", "<http://xmlns.com/foaf/0.1/name>", "Bob", ""),
          ("_:c", "<http://xmlns.com/foaf/0.1/name>", "Charles", "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?x ?name
            |WHERE   {
            |   ?x foaf:name ?name .
            |   FILTER(?name >= "Bob")
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:b", "\"Bob\""),
          Row("_:c", "\"Charles\"")
        )
      }

      // TODO: Add support for string syntactic sugar, see: https://lists.w3.org/Archives/Public/public-sparql-dev/2013AprJun/0003.html
      "execute on strings" ignore {

        val df: DataFrame = List(
          (
            "_:a",
            "<http://xmlns.com/foaf/0.1/name>",
            "\"Anthony\"^^<http://www.w3.org/2001/XMLSchema#string>",
            ""
          ),
          (
            "_:b",
            "<http://xmlns.com/foaf/0.1/name>",
            "\"Bob\"^^<http://www.w3.org/2001/XMLSchema#string>",
            ""
          ),
          (
            "_:c",
            "<http://xmlns.com/foaf/0.1/name>",
            "\"Charles\"^^<http://www.w3.org/2001/XMLSchema#string>",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            |
            |SELECT ?x ?name
            |WHERE   {
            |   ?x foaf:name ?name .
            |   FILTER(?name >= "Bob"^^xsd:string)
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:b", "\"Bob\"^^xsd:string"),
          Row("_:c", "\"Charles\"^^xsd:string")
        )
      }

      "execute on numbers" in {

        val df: DataFrame = List(
          ("_:Bob", "<http://xmlns.com/foaf/0.1/age>", 15, ""),
          ("_:Alice", "<http://xmlns.com/foaf/0.1/age>", 18, ""),
          ("_:Henry", "<http://xmlns.com/foaf/0.1/age>", 21, "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?x ?age
            |WHERE   {
            |   ?x foaf:age ?age .
            |   FILTER(?age >= 18)
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:Alice", "18"),
          Row("_:Henry", "21")
        )
      }

      "execute on booleans" in {

        val df: DataFrame = List(
          ("_:Martha", "<http://xmlns.com/foaf/0.1/isFemale>", true, ""),
          ("_:Henry", "<http://xmlns.com/foaf/0.1/isFemale>", false, "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?x ?isFemale
            |WHERE   {
            |   ?x foaf:isFemale ?isFemale .
            |   FILTER(?isFemale >= true)
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:Martha", "true")
        )
      }

      "execute on dateTimes" in {

        val df: DataFrame = List(
          ("_:Martha", "<http://xmlns.com/foaf/0.1/isFemale>", "true", ""),
          ("_:Henry", "<http://xmlns.com/foaf/0.1/isFemale>", "false", ""),
          ("_:Ana", "<http://xmlns.com/foaf/0.1/isFemale>", "true", ""),
          (
            "_:Martha",
            "<http://xmlns.com/foaf/0.1/birthDay>",
            """"2000-10-10T10:10:10.000"^^xsd:dateTime""",
            ""
          ),
          (
            "_:Ana",
            "<http://xmlns.com/foaf/0.1/birthDay>",
            """"2000-10-10T10:10:10.000"^^xsd:dateTime""",
            ""
          ),
          (
            "_:Henry",
            "<http://xmlns.com/foaf/0.1/birthDay>",
            """"1990-10-10T10:10:10.000"^^xsd:dateTime""",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
            |
            |SELECT ?x
            |WHERE   {
            |   ?x foaf:birthDay ?bday .
            |   FILTER(?bday >= "1990-10-10T10:10:10.000"^^xsd:dateTime)
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect should have length 3
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:Martha"),
          Row("_:Ana"),
          Row("_:Henry")
        )
      }
    }

    "logical operation LTE" should {

      "execute on simple literal" in {

        val df: DataFrame = List(
          ("_:a", "<http://xmlns.com/foaf/0.1/name>", "Anthony", ""),
          ("_:b", "<http://xmlns.com/foaf/0.1/name>", "Bob", ""),
          ("_:c", "<http://xmlns.com/foaf/0.1/name>", "Charles", "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?x ?name
            |WHERE   {
            |   ?x foaf:name ?name .
            |   FILTER(?name <= "Bob")
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:a", "\"Anthony\""),
          Row("_:b", "\"Bob\"")
        )
      }

      // TODO: Add support for string syntactic sugar, see: https://lists.w3.org/Archives/Public/public-sparql-dev/2013AprJun/0003.html
      "execute on strings" ignore {

        val df: DataFrame = List(
          (
            "_:a",
            "<http://xmlns.com/foaf/0.1/name>",
            "\"Anthony\"^^<http://www.w3.org/2001/XMLSchema#string>",
            ""
          ),
          (
            "_:b",
            "<http://xmlns.com/foaf/0.1/name>",
            "\"Bob\"^^<http://www.w3.org/2001/XMLSchema#string>",
            ""
          ),
          (
            "_:c",
            "<http://xmlns.com/foaf/0.1/name>",
            "\"Charles\"^^<http://www.w3.org/2001/XMLSchema#string>",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            |
            |SELECT ?x ?name
            |WHERE   {
            |   ?x foaf:name ?name .
            |   FILTER(?name <= "Bob"^^xsd:string)
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:a", "\"Anthony\"^^xsd:string"),
          Row("_:b", "\"Bob\"^^xsd:string")
        )
      }

      "execute on numbers" in {

        val df: DataFrame = List(
          ("_:Bob", "<http://xmlns.com/foaf/0.1/age>", 15, ""),
          ("_:Alice", "<http://xmlns.com/foaf/0.1/age>", 18, ""),
          ("_:Henry", "<http://xmlns.com/foaf/0.1/age>", 21, "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?x ?age
            |WHERE   {
            |   ?x foaf:age ?age .
            |   FILTER(?age <= 18)
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:Bob", "15"),
          Row("_:Alice", "18")
        )
      }

      "execute on booleans" in {

        val df: DataFrame = List(
          ("_:Martha", "<http://xmlns.com/foaf/0.1/isFemale>", true, ""),
          ("_:Henry", "<http://xmlns.com/foaf/0.1/isFemale>", false, "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?x ?isFemale
            |WHERE   {
            |   ?x foaf:isFemale ?isFemale .
            |   FILTER(?isFemale <= true)
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:Martha", "true"),
          Row("_:Henry", "false")
        )
      }

      // TODO: Implement Date Time support issue
      "execute on dateTimes" in {

        val df: DataFrame = List(
          ("_:Martha", "<http://xmlns.com/foaf/0.1/isFemale>", "true", ""),
          ("_:Henry", "<http://xmlns.com/foaf/0.1/isFemale>", "false", ""),
          ("_:Ana", "<http://xmlns.com/foaf/0.1/isFemale>", "true", ""),
          (
            "_:Martha",
            "<http://xmlns.com/foaf/0.1/birthDay>",
            """"2000-10-10T10:10:10.000"^^xsd:dateTime""",
            ""
          ),
          (
            "_:Ana",
            "<http://xmlns.com/foaf/0.1/birthDay>",
            """"2000-10-10T10:10:10.000"^^xsd:dateTime""",
            ""
          ),
          (
            "_:Henry",
            "<http://xmlns.com/foaf/0.1/birthDay>",
            """"1990-10-10T10:10:10.000"^^xsd:dateTime""",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
            |
            |SELECT ?x
            |WHERE   {
            |   ?x foaf:birthDay ?bday .
            |   FILTER(?bday <= "2000-10-10T10:10:10.000"^^xsd:dateTime)
            |}
            |
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect should have length 3
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:Martha"),
          Row("_:Ana"),
          Row("_:Henry")
        )
      }
    }
  }
}
