package com.gsk.kg.engine

import org.apache.jena.riot.RDFParser
import org.apache.jena.riot.lang.CollectorStreamTriples

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CompilerSpec extends AnyWordSpec with Matchers with DataFrameSuiteBase {

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  val dfList = List(
    (
      "test",
      "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
      "http://id.gsk.com/dm/1.0/Document",
      ""
    ),
    ("test", "http://id.gsk.com/dm/1.0/docSource", "source", "")
  )

  "Compiler" when {

    "format data type literals correctly" in {
      import sqlContext.implicits._

      val df: DataFrame = List(
        (
          "example",
          "http://xmlns.com/foaf/0.1/lit",
          "\"5.88\"^^<http://www.w3.org/2001/XMLSchema#float>",
          ""
        ),
        ("example", "http://xmlns.com/foaf/0.1/lit", "\"0.22\"^^xsd:float", ""),
        ("example", "http://xmlns.com/foaf/0.1/lit", "\"foo\"^^xsd:string", ""),
        (
          "example",
          "http://xmlns.com/foaf/0.1/lit",
          "\"true\"^^xsd:boolean",
          ""
        )
      ).toDF("s", "p", "o", "g")

      val query =
        """
          |PREFIX foaf:   <http://xmlns.com/foaf/0.1/>
          |PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>
          |
          |SELECT   ?lit
          |WHERE    {
          |  ?x foaf:lit ?lit .
          |}
          |""".stripMargin

      val result = Compiler.compile(df, query)

      result shouldBe a[Right[_, _]]
      result.right.get.collect().length shouldEqual 4
      result.right.get.collect().toSet shouldEqual Set(
        Row("\"5.88\"^^<http://www.w3.org/2001/XMLSchema#float>"),
        Row("\"0.22\"^^xsd:float"),
        Row("\"foo\"^^xsd:string"),
        Row("\"true\"^^xsd:boolean")
      )
    }

    /** TODO(pepegar): In order to make this test pass we need the
      * results to be RDF compliant (mainly, wrapping values correctly)
      */
    "query a real DF with a real query" in {
      val query =
        """
      PREFIX  schema: <http://schema.org/>
      PREFIX  rdf:  <http://www.w3.org/2000/01/rdf-schema#>
      PREFIX  xml:  <http://www.w3.org/XML/1998/namespace>
      PREFIX  dm:   <http://gsk-kg.rdip.gsk.com/dm/1.0/>
      PREFIX  prism: <http://prismstandard.org/namespaces/basic/2.0/>
      PREFIX  litg:  <http://lit-search-api/graph/>
      PREFIX  litn:  <http://lit-search-api/node/>
      PREFIX  lite:  <http://lit-search-api/edge/>
      PREFIX  litp:  <http://lit-search-api/property/>

      CONSTRUCT {
        ?Document a litn:Document .
        ?Document litp:docID ?docid .
      }
      WHERE{
        ?d a dm:Document .
        BIND(STRAFTER(str(?d), "#") as ?docid) .
        BIND(URI(CONCAT("http://lit-search-api/node/doc#", ?docid)) as ?Document) .
      }
      """

      val inputDF  = readNTtoDF("fixtures/reference-q1-input.nt")
      val outputDF = readNTtoDF("fixtures/reference-q1-output.nt")

      val result = Compiler.compile(inputDF, query)

      result shouldBe a[Right[_, _]]
      result.right.get.collect.toSet shouldEqual outputDF
        .drop("g")
        .collect()
        .toSet
    }

    "perform query with BGPs" should {

      "will execute operations in the dataframe" in {
        import sqlContext.implicits._

        val df = dfList.toDF("s", "p", "o", "g")
        val query =
          """
            SELECT
              ?s ?p ?o
            WHERE {
              ?s ?p ?o .
            }
            """

        Compiler.compile(df, query).right.get.collect() shouldEqual Array(
          Row(
            "\"test\"",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://id.gsk.com/dm/1.0/Document"
          ),
          Row("\"test\"", "http://id.gsk.com/dm/1.0/docSource", "\"source\"")
        )
      }

      "will execute with two dependent BGPs" in {
        import sqlContext.implicits._

        val df: DataFrame = dfList.toDF("s", "p", "o", "g")

        val query =
          """
            SELECT
              ?d ?src
            WHERE {
              ?d a <http://id.gsk.com/dm/1.0/Document> .
              ?d <http://id.gsk.com/dm/1.0/docSource> ?src
            }
            """

        Compiler.compile(df, query).right.get.collect() shouldEqual Array(
          Row("\"test\"", "\"source\"")
        )
      }
    }

    "perform query with UNION statement" should {

      "execute with the same bindings" in {
        import sqlContext.implicits._

        val df: DataFrame = (("does", "not", "match", "") :: dfList)
          .toDF("s", "p", "o", "g")

        val query =
          """
      SELECT
        ?s ?o
      WHERE {
        { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o }
        UNION
        { ?s <http://id.gsk.com/dm/1.0/docSource> ?o }
      }
      """

        Compiler.compile(df, query).right.get.collect() shouldEqual Array(
          Row("\"test\"", "http://id.gsk.com/dm/1.0/Document"),
          Row("\"test\"", "\"source\"")
        )
      }

      "execute with different bindings" in {
        import sqlContext.implicits._

        val df: DataFrame =
          (("does", "not", "match", "") :: dfList).toDF("s", "p", "o", "g")

        val query =
          """
      SELECT
        ?s ?o ?s2 ?o2
      WHERE {
        { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o }
        UNION
        { ?s2 <http://id.gsk.com/dm/1.0/docSource> ?o2 }
      }
      """

        Compiler.compile(df, query).right.get.collect() shouldEqual Array(
          Row("\"test\"", "http://id.gsk.com/dm/1.0/Document", null, null),
          Row(null, null, "\"test\"", "\"source\"")
        )
      }
    }

    "perform with CONSTRUCT statement" should {

      "execute with a single triple pattern" in {
        import sqlContext.implicits._

        val df: DataFrame = dfList.toDF("s", "p", "o", "g")

        val query =
          """
        CONSTRUCT {
          ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o
        } WHERE {
          ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o
        }
        """

        Compiler.compile(df, query).right.get.collect() shouldEqual Array(
          Row(
            "\"test\"",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://id.gsk.com/dm/1.0/Document"
          )
        )
      }

      "execute with more than one triple pattern" in {
        import sqlContext.implicits._

        val positive = List(
          (
            "doesmatch",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://id.gsk.com/dm/1.0/Document",
            ""
          ),
          (
            "doesmatchaswell",
            "http://id.gsk.com/dm/1.0/docSource",
            "potato",
            ""
          )
        )
        val df: DataFrame = (positive ++ dfList).toDF("s", "p", "o", "g")

        val query =
          """
      CONSTRUCT {
        ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o .
        ?s2 <http://id.gsk.com/dm/1.0/docSource> ?o2
      } WHERE {
        ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o .
        ?s2 <http://id.gsk.com/dm/1.0/docSource> ?o2
      }
      """

        Compiler.compile(df, query).right.get.collect().toSet shouldEqual Set(
          Row(
            "\"doesmatch\"",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://id.gsk.com/dm/1.0/Document"
          ),
          Row(
            "\"doesmatchaswell\"",
            "http://id.gsk.com/dm/1.0/docSource",
            "\"potato\""
          ),
          Row(
            "\"test\"",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://id.gsk.com/dm/1.0/Document"
          ),
          Row(
            "\"test\"",
            "http://id.gsk.com/dm/1.0/docSource",
            "\"source\""
          )
        )
      }

      "execute with more than one triple pattern with common bindings" in {
        import sqlContext.implicits._

        val negative = List(
          (
            "doesntmatch",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://id.gsk.com/dm/1.0/Document",
            ""
          ),
          (
            "doesntmatcheither",
            "http://id.gsk.com/dm/1.0/docSource",
            "potato",
            ""
          )
        )

        val df: DataFrame = (negative ++ dfList).toDF("s", "p", "o", "g")

        val query =
          """
      CONSTRUCT
      {
        ?d <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://id.gsk.com/dm/1.0/Document> .
        ?d <http://id.gsk.com/dm/1.0/docSource> ?src
      }
      WHERE
      {
        ?d <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://id.gsk.com/dm/1.0/Document> .
        ?d <http://id.gsk.com/dm/1.0/docSource> ?src
      }
      """

        Compiler.compile(df, query).right.get.collect().toSet shouldEqual Set(
          Row(
            "\"test\"",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://id.gsk.com/dm/1.0/Document"
          ),
          Row(
            "\"test\"",
            "http://id.gsk.com/dm/1.0/docSource",
            "\"source\""
          )
        )
      }
    }

    "perform query with LIMIT modifier" should {

      "execute with limit greater than 0" in {
        import sqlContext.implicits._

        val df: DataFrame = List(
          ("a", "b", "c", ""),
          ("team", "http://xmlns.com/foaf/0.1/name", "Anthony", ""),
          ("team", "http://xmlns.com/foaf/0.1/name", "Perico", ""),
          ("team", "http://xmlns.com/foaf/0.1/name", "Henry", "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT  ?name
            |WHERE   { ?x foaf:name ?name }
            |LIMIT   2
            |""".stripMargin

        val result = Compiler.compile(df, query)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"Anthony\""),
          Row("\"Perico\"")
        )
      }

      "execute with limit equal to 0 and obtain no results" in {
        import sqlContext.implicits._

        val df: DataFrame = List(
          ("a", "b", "c", ""),
          ("team", "http://xmlns.com/foaf/0.1/name", "Anthony", ""),
          ("team", "http://xmlns.com/foaf/0.1/name", "Perico", ""),
          ("team", "http://xmlns.com/foaf/0.1/name", "Henry", "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT  ?name
            |WHERE   { ?x foaf:name ?name }
            |LIMIT   0
            |""".stripMargin

        val result = Compiler.compile(df, query)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 0
        result.right.get.collect.toSet shouldEqual Set.empty
      }

      "execute with limit greater than Java MAX INTEGER and obtain an error" in {
        import sqlContext.implicits._

        val df: DataFrame = List(
          ("a", "b", "c", ""),
          ("team", "http://xmlns.com/foaf/0.1/name", "Anthony", ""),
          ("team", "http://xmlns.com/foaf/0.1/name", "Perico", ""),
          ("team", "http://xmlns.com/foaf/0.1/name", "Henry", "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT  ?name
            |WHERE   { ?x foaf:name ?name }
            |LIMIT   2147483648
            |""".stripMargin

        val result = Compiler.compile(df, query)

        result shouldBe a[Left[_, _]]
        result.left.get shouldEqual EngineError.NumericTypesDoNotMatch(
          "2147483648 to big to be converted to an Int"
        )
      }
    }

    "perform query with OFFSET modifier" should {

      "execute with offset greater than 0 and obtain a non empty set" in {
        import sqlContext.implicits._

        val df: DataFrame = List(
          ("a", "b", "c", ""),
          ("team", "http://xmlns.com/foaf/0.1/name", "Anthony", ""),
          ("team", "http://xmlns.com/foaf/0.1/name", "Perico", ""),
          ("team", "http://xmlns.com/foaf/0.1/name", "Henry", "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT  ?name
            |WHERE   { ?x foaf:name ?name }
            |OFFSET 1
            |""".stripMargin

        val result = Compiler.compile(df, query)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"Perico\""),
          Row("\"Henry\"")
        )
      }

      "execute with offset equal to 0 and obtain same elements as the original set" in {
        import sqlContext.implicits._

        val df: DataFrame = List(
          ("a", "b", "c", ""),
          ("team", "http://xmlns.com/foaf/0.1/name", "Anthony", ""),
          ("team", "http://xmlns.com/foaf/0.1/name", "Perico", ""),
          ("team", "http://xmlns.com/foaf/0.1/name", "Henry", "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT  ?name
            |WHERE   { ?x foaf:name ?name }
            |OFFSET 0
            |""".stripMargin

        val result = Compiler.compile(df, query)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 3
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"Anthony\""),
          Row("\"Perico\""),
          Row("\"Henry\"")
        )
      }

      "execute with offset greater than the number of elements of the dataframe and obtain an empty set" in {
        import sqlContext.implicits._

        val df: DataFrame = List(
          ("a", "b", "c", ""),
          ("team", "http://xmlns.com/foaf/0.1/name", "Anthony", ""),
          ("team", "http://xmlns.com/foaf/0.1/name", "Perico", ""),
          ("team", "http://xmlns.com/foaf/0.1/name", "Henry", "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT  ?name
            |WHERE   { ?x foaf:name ?name }
            |OFFSET 5
            |""".stripMargin

        val result = Compiler.compile(df, query)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 0
        result.right.get.collect.toSet shouldEqual Set.empty
      }
    }

    "perform query with Blank nodes" should {

      "execute and obtain expected results" in {
        import sqlContext.implicits._

        val df: DataFrame = List(
          (
            "nodeA",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/predEntityClass",
            "thisIsTheBlankNode",
            ""
          ),
          (
            "thisIsTheBlankNode",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/predClass",
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

        val result = Compiler.compile(df, query)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"nodeA\"", "\"otherThingy\"")
        )
      }
    }

    "perform query with REPLACE function" should {

      "execute and obtain expected results" in {
        import sqlContext.implicits._

        val df: DataFrame = List(
          ("example", "http://xmlns.com/foaf/0.1/lit", "abcd", ""),
          ("example", "http://xmlns.com/foaf/0.1/lit", "abaB", ""),
          ("example", "http://xmlns.com/foaf/0.1/lit", "bbBB", ""),
          ("example", "http://xmlns.com/foaf/0.1/lit", "aaaa", "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:   <http://xmlns.com/foaf/0.1/>
            |
            |SELECT   ?lit ?lit2
            |WHERE    {
            |  ?x foaf:lit ?lit .
            |  BIND(REPLACE(?lit, "b", "Z") AS ?lit2)
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 4
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"abcd\"", "\"aZcd\""),
          Row("\"abaB\"", "\"aZaB\""),
          Row("\"bbBB\"", "\"ZZBB\""),
          Row("\"aaaa\"", "\"aaaa\"")
        )
      }

      // TODO: Capture Spark exceptions on the Left projection once the query trigger the Spark action
      // this can be done probably at the Engine level
      "execute and obtain an expected error, " +
        "because the pattern matches the zero-length string" ignore {
          import sqlContext.implicits._

          val df: DataFrame = List(
            ("example", "http://xmlns.com/foaf/0.1/lit", "abracadabra", "")
          ).toDF("s", "p", "o", "g")

          val query =
            """
            |PREFIX foaf:   <http://xmlns.com/foaf/0.1/>
            |
            |SELECT   ?lit ?lit2
            |WHERE    {
            |  ?x foaf:lit ?lit .
            |  BIND(REPLACE(?lit, ".*?", "$1") AS ?lit2)
            |}
            |""".stripMargin

          val result = Compiler.compile(df, query)

          result shouldBe a[Left[_, _]]
          result.left.get shouldEqual EngineError.FunctionError(
            s"Error on REPLACE function: No group 1"
          )
        }
    }

    "perform query with ISBLANK function" should {

      "execute and obtain expected results" in {
        import sqlContext.implicits._

        val df: DataFrame = List(
          (
            "_:a",
            "http://www.w3.org/2000/10/annotation-ns#annotates",
            "http://www.w3.org/TR/rdf-sparql-query/",
            ""
          ),
          (
            "_:a",
            "http://purl.org/dc/elements/1.1/creator",
            "Alice B. Toeclips",
            ""
          ),
          (
            "_:b",
            "http://www.w3.org/2000/10/annotation-ns#annotates",
            "http://www.w3.org/TR/rdf-sparql-query/",
            ""
          ),
          ("_:b", "http://purl.org/dc/elements/1.1/creator", "_:c", ""),
          ("_:c", "http://xmlns.com/foaf/0.1/given", "Bob", ""),
          ("_:c", "http://xmlns.com/foaf/0.1/family", "Smith", "")
        ).toDF("s", "p", "o", "g")

        val query = {
          """
            |PREFIX a:      <http://www.w3.org/2000/10/annotation-ns#>
            |PREFIX dc:     <http://purl.org/dc/elements/1.1/>
            |PREFIX foaf:   <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?given ?family
            |WHERE { ?annot  a:annotates  <http://www.w3.org/TR/rdf-sparql-query/> .
            |  ?annot  dc:creator   ?c .
            |  OPTIONAL { ?c  foaf:given   ?given ; foaf:family  ?family } .
            |  FILTER isBlank(?c)
            |}
            |""".stripMargin
        }

        val result = Compiler.compile(df, query)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"Bob\"", "\"Smith\"")
        )
      }
    }

    "perform query with FILTER modifier" which {

      "with single condition" should {

        "execute and obtain expected results" in {

          import sqlContext.implicits._

          val df: DataFrame = List(
            ("a", "b", "c", ""),
            ("team", "http://xmlns.com/foaf/0.1/name", "Anthony", ""),
            ("team", "http://xmlns.com/foaf/0.1/name", "Perico", ""),
            ("team", "http://xmlns.com/foaf/0.1/name", "Henry", ""),
            ("_:", "http://xmlns.com/foaf/0.1/name", "Blank", "")
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(Row("\"Blank\""))
        }

        "execute and obtain expected results when condition has embedded functions" in {

          import sqlContext.implicits._

          val df: DataFrame = List(
            ("a", "b", "c", ""),
            ("team", "http://xmlns.com/foaf/0.1/name", "Anthony", ""),
            ("team", "http://xmlns.com/foaf/0.1/name", "Perico", ""),
            ("team", "http://xmlns.com/foaf/0.1/name", "Henry", ""),
            ("a:", "http://xmlns.com/foaf/0.1/name", "Blank", "")
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(Row("\"Blank\""))
        }

        "execute and obtain expected results when double filter" in {

          import sqlContext.implicits._

          val df: DataFrame = List(
            ("a", "b", "c", ""),
            ("team", "http://xmlns.com/foaf/0.1/name", "Anthony", ""),
            ("_:a", "http://xmlns.com/foaf/0.1/name", "_:b", ""),
            ("foaf:c", "http://xmlns.com/foaf/0.1/name", "_:d", ""),
            ("_:e", "http://xmlns.com/foaf/0.1/name", "foaf:f", "")
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:a", "_:b")
          )
        }

        "execute and obtain expected results when filter over all select statement" in {

          import sqlContext.implicits._

          val df: DataFrame = List(
            (
              "http://example.org/Network",
              "http://www.w3.org/2000/01/rdf-schema#subClassOf",
              "http://example.org/Main",
              ""
            ),
            (
              "http://example.org/ATM",
              "http://www.w3.org/2000/01/rdf-schema#subClassOf",
              "http://example.org/Network",
              ""
            ),
            (
              "http://example.org/ARPANET",
              "http://www.w3.org/2000/01/rdf-schema#subClassOf",
              "http://example.org/Network",
              ""
            ),
            (
              "http://example.org/Software",
              "http://www.w3.org/2000/01/rdf-schema#subClassOf",
              "http://example.org/Main",
              ""
            ),
            (
              "_:Linux",
              "http://www.w3.org/2000/01/rdf-schema#subClassOf",
              "http://example.org/Software",
              ""
            ),
            (
              "http://example.org/Windows",
              "http://www.w3.org/2000/01/rdf-schema#subClassOf",
              "http://example.org/Software",
              ""
            ),
            (
              "http://example.org/XP",
              "http://www.w3.org/2000/01/rdf-schema#subClassOf",
              "http://example.org/Windows",
              ""
            ),
            (
              "http://example.org/Win7",
              "http://www.w3.org/2000/01/rdf-schema#subClassOf",
              "http://example.org/Windows",
              ""
            ),
            (
              "http://example.org/Win8",
              "http://www.w3.org/2000/01/rdf-schema#subClassOf",
              "http://example.org/Windows",
              ""
            ),
            (
              "http://example.org/Ubuntu20",
              "http://www.w3.org/2000/01/rdf-schema#subClassOf",
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("http://example.org/Windows")
          )
        }

        // TODO: Un-ignore when implemented EQUALS and GT
        "execute and obtain expected results when complex filter" ignore {

          import sqlContext.implicits._

          val df: DataFrame = List(
            ("_:a", "http://xmlns.com/foaf/0.1/name", "Alice", ""),
            (
              "_:a",
              "http://example.org/stats#hits",
              "\"2349\"^^xsd:integer",
              ""
            ),
            ("_:b", "http://xmlns.com/foaf/0.1/name", "Bob", ""),
            (
              "_:b",
              "http://example.org/stats#hits",
              "\"105\"^^xsd:integer",
              ""
            ),
            ("_:c", "http://xmlns.com/foaf/0.1/name", "Eve"),
            ("_:c", "http://example.org/stats#hits", "\"181\"^^xsd:integer", "")
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 2
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:a", "foaf:name", "\"Bob\"", ""),
            Row("_:b", "site:hits", "\"2349\"^^xsd:integer", "")
          )
        }
      }

      "with multiple conditions" should {

        // TODO: Un-ignore when binary logical operations implemented
        "execute and obtain expected results when multiple conditions" ignore {

          import sqlContext.implicits._

          val df: DataFrame = List(
            ("a", "b", "c", ""),
            ("team", "http://xmlns.com/foaf/0.1/name", "Anthony", ""),
            ("team", "http://xmlns.com/foaf/0.1/name", "Perico", ""),
            ("team", "http://xmlns.com/foaf/0.1/name", "Henry", ""),
            ("_:", "http://xmlns.com/foaf/0.1/name", "Blank", ""),
            (
              "_:",
              "http://xmlns.com/foaf/0.1/name",
              "http://test-uri/blank",
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
              |   FILTER (isBlank(?x) && isURI(?x))
              |}
              |
              |""".stripMargin

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(Row("\"Blank\""))
        }

        "execute and obtain expected results when there is AND condition" in {
          import sqlContext.implicits._

          val df: DataFrame = List(
            ("team", "http://xmlns.com/foaf/0.1/name", "_:", ""),
            ("team", "http://xmlns.com/foaf/0.1/name", "Perico", ""),
            ("_:", "http://xmlns.com/foaf/0.1/name", "_:", ""),
            ("_:", "http://xmlns.com/foaf/0.1/name", "Henry", "")
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:", "\"Henry\"")
          )
        }

        "execute and obtain expected results when there is OR condition" in {
          import sqlContext.implicits._

          val df: DataFrame = List(
            ("team", "http://xmlns.com/foaf/0.1/name", "_:", ""),
            ("team", "http://xmlns.com/foaf/0.1/name", "Perico", ""),
            ("_:", "http://xmlns.com/foaf/0.1/name", "_:", ""),
            ("_:", "http://xmlns.com/foaf/0.1/name", "Henry", "")
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 3
          result.right.get.collect.toSet shouldEqual Set(
            Row("\"team\"", "_:"),
            Row("_:", "_:"),
            Row("_:", "\"Henry\"")
          )
        }
      }

      "with logical operation EQUALS" should {

        "execute on simple literal" in {
          import sqlContext.implicits._

          val df: DataFrame = List(
            ("_:a", "http://xmlns.com/foaf/0.1/name", "Henry", ""),
            ("_:b", "http://xmlns.com/foaf/0.1/name", "Perico", "")
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:a", "\"Henry\"")
          )
        }

        // TODO: Add support for string syntactic sugar, see: https://lists.w3.org/Archives/Public/public-sparql-dev/2013AprJun/0003.html
        "execute on strings" ignore {
          import sqlContext.implicits._

          val df: DataFrame = List(
            (
              "_:a",
              "http://xmlns.com/foaf/0.1/name",
              "\"Henry\"^^<http://www.w3.org/2001/XMLSchema#string>",
              ""
            ),
            (
              "_:b",
              "http://xmlns.com/foaf/0.1/name",
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row(":_a", "\"Henry\"^^xsd:string")
          )
        }

        "execute on numbers" in {
          import sqlContext.implicits._

          val df: DataFrame = List(
            ("_:Perico", "http://xmlns.com/foaf/0.1/age", 15, ""),
            ("_:Henry", "http://xmlns.com/foaf/0.1/age", 21, "")
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:Henry", 21)
          )
        }

        "execute on booleans" in {
          import sqlContext.implicits._

          val df: DataFrame = List(
            ("_:Martha", "http://xmlns.com/foaf/0.1/isFemale", true, ""),
            ("_:Henry", "http://xmlns.com/foaf/0.1/isFemale", false, "")
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:Martha", true)
          )
        }

        "execute on datetimes" in {
          import sqlContext.implicits._

          val df: DataFrame = List(
            ("_:Martha", "http://xmlns.com/foaf/0.1/isFemale", "true", ""),
            ("_:Henry", "http://xmlns.com/foaf/0.1/isFemale", "false", ""),
            ("_:Ana", "http://xmlns.com/foaf/0.1/isFemale", "true", ""),
            (
              "_:Martha",
              "http://xmlns.com/foaf/0.1/birthDay",
              """"2000-10-10T10:10:10.000"^^xsd:dateTime""",
              ""
            ),
            (
              "_:Ana",
              "http://xmlns.com/foaf/0.1/birthDay",
              """"2000-10-10T10:10:10.000"^^xsd:dateTime""",
              ""
            ),
            (
              "_:Henry",
              "http://xmlns.com/foaf/0.1/birthDay",
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect should have length 2
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:Martha"),
            Row("_:Ana")
          )
        }
      }

      "with logical operation NOT EQUALS" should {

        "execute on simple literal" in {
          import sqlContext.implicits._

          val df: DataFrame = List(
            ("_:a", "http://xmlns.com/foaf/0.1/name", "Henry", ""),
            ("_:b", "http://xmlns.com/foaf/0.1/name", "Perico", "")
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:b", "\"Perico\"")
          )
        }

        // TODO: Add support for string syntactic sugar, see: https://lists.w3.org/Archives/Public/public-sparql-dev/2013AprJun/0003.html
        "execute on strings" ignore {
          import sqlContext.implicits._

          val df: DataFrame = List(
            (
              "_:a",
              "http://xmlns.com/foaf/0.1/name",
              "\"Henry\"^^<http://www.w3.org/2001/XMLSchema#string>",
              ""
            ),
            (
              "_:b",
              "http://xmlns.com/foaf/0.1/name",
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row(":_b", "\"Perico\"^^xsd:string")
          )
        }

        "execute on numbers" in {
          import sqlContext.implicits._

          val df: DataFrame = List(
            ("_:Perico", "http://xmlns.com/foaf/0.1/age", 15, ""),
            ("_:Henry", "http://xmlns.com/foaf/0.1/age", 21, "")
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:Perico", 15)
          )
        }

        "execute on booleans" in {
          import sqlContext.implicits._

          val df: DataFrame = List(
            ("_:Martha", "http://xmlns.com/foaf/0.1/isFemale", true, ""),
            ("_:Henry", "http://xmlns.com/foaf/0.1/isFemale", false, "")
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:Henry", false)
          )
        }

        "execute on dateTimes" in {
          import sqlContext.implicits._

          val df: DataFrame = List(
            ("_:Martha", "http://xmlns.com/foaf/0.1/isFemale", "true", ""),
            ("_:Henry", "http://xmlns.com/foaf/0.1/isFemale", "false", ""),
            ("_:Ana", "http://xmlns.com/foaf/0.1/isFemale", "true", ""),
            (
              "_:Martha",
              "http://xmlns.com/foaf/0.1/birthDay",
              """"2000-10-10T10:10:10.000"^^xsd:dateTime""",
              ""
            ),
            (
              "_:Ana",
              "http://xmlns.com/foaf/0.1/birthDay",
              """"2000-10-10T10:10:10.000"^^xsd:dateTime""",
              ""
            ),
            (
              "_:Henry",
              "http://xmlns.com/foaf/0.1/birthDay",
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect should have length 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:Henry")
          )
        }
      }

      "with logical operation GT" should {

        "execute on simple literal" in {
          import sqlContext.implicits._

          val df: DataFrame = List(
            ("_:a", "http://xmlns.com/foaf/0.1/name", "Anthony", ""),
            ("_:b", "http://xmlns.com/foaf/0.1/name", "Charles", "")
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:b", "\"Charles\"")
          )
        }

        // TODO: Add support for string syntactic sugar, see: https://lists.w3.org/Archives/Public/public-sparql-dev/2013AprJun/0003.html
        "execute on strings" ignore {
          import sqlContext.implicits._

          val df: DataFrame = List(
            (
              "_:a",
              "http://xmlns.com/foaf/0.1/name",
              "\"Anthony\"^^<http://www.w3.org/2001/XMLSchema#string>"
            ),
            (
              "_:b",
              "http://xmlns.com/foaf/0.1/name",
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:b", "\"Charles\"^^xsd:string")
          )
        }

        "execute on numbers" in {
          import sqlContext.implicits._

          val df: DataFrame = List(
            ("_:Bob", "http://xmlns.com/foaf/0.1/age", 15, ""),
            ("_:Alice", "http://xmlns.com/foaf/0.1/age", 18, ""),
            ("_:Henry", "http://xmlns.com/foaf/0.1/age", 21, "")
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:Henry", 21)
          )
        }

        "execute on booleans" in {
          import sqlContext.implicits._

          val df: DataFrame = List(
            ("_:Martha", "http://xmlns.com/foaf/0.1/isFemale", true, ""),
            ("_:Henry", "http://xmlns.com/foaf/0.1/isFemale", false, "")
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 0
          result.right.get.collect.toSet shouldEqual Set()
        }

        // TODO: Implement Date Time support issue
        "execute on dateTimes" in {
          import sqlContext.implicits._

          val df: DataFrame = List(
            ("_:Martha", "http://xmlns.com/foaf/0.1/isFemale", "true", ""),
            ("_:Henry", "http://xmlns.com/foaf/0.1/isFemale", "false", ""),
            ("_:Ana", "http://xmlns.com/foaf/0.1/isFemale", "true", ""),
            (
              "_:Martha",
              "http://xmlns.com/foaf/0.1/birthDay",
              """"2000-10-10T10:10:10.000"^^xsd:dateTime""",
              ""
            ),
            (
              "_:Ana",
              "http://xmlns.com/foaf/0.1/birthDay",
              """"2000-10-10T10:10:10.000"^^xsd:dateTime""",
              ""
            ),
            (
              "_:Henry",
              "http://xmlns.com/foaf/0.1/birthDay",
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect should have length 2
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:Martha"),
            Row("_:Ana")
          )
        }
      }

      "with logical operation LT" should {

        "execute on simple literal" in {
          import sqlContext.implicits._

          val df: DataFrame = List(
            ("_:a", "http://xmlns.com/foaf/0.1/name", "Anthony", ""),
            ("_:b", "http://xmlns.com/foaf/0.1/name", "Charles", "")
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:a", "\"Anthony\"")
          )
        }

        // TODO: Add support for string syntactic sugar, see: https://lists.w3.org/Archives/Public/public-sparql-dev/2013AprJun/0003.html
        "execute on strings" ignore {
          import sqlContext.implicits._

          val df: DataFrame = List(
            (
              "_:a",
              "http://xmlns.com/foaf/0.1/name",
              "\"Anthony\"^^<http://www.w3.org/2001/XMLSchema#string>",
              ""
            ),
            (
              "_:b",
              "http://xmlns.com/foaf/0.1/name",
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:a", "\"Anthony\"^^xsd:string")
          )
        }

        "execute on numbers" in {
          import sqlContext.implicits._

          val df: DataFrame = List(
            ("_:Bob", "http://xmlns.com/foaf/0.1/age", 15, ""),
            ("_:Alice", "http://xmlns.com/foaf/0.1/age", 18, ""),
            ("_:Henry", "http://xmlns.com/foaf/0.1/age", 21, "")
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:Bob", 15)
          )
        }

        "execute on booleans" in {
          import sqlContext.implicits._

          val df: DataFrame = List(
            ("_:Martha", "http://xmlns.com/foaf/0.1/isFemale", true, ""),
            ("_:Henry", "http://xmlns.com/foaf/0.1/isFemale", false, "")
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:Henry", false)
          )
        }

        // TODO: Implement Date Time support issue
        "execute on dateTimes" in {
          import sqlContext.implicits._

          val df: DataFrame = List(
            ("_:Martha", "http://xmlns.com/foaf/0.1/isFemale", "true", ""),
            ("_:Henry", "http://xmlns.com/foaf/0.1/isFemale", "false", ""),
            ("_:Ana", "http://xmlns.com/foaf/0.1/isFemale", "true", ""),
            (
              "_:Martha",
              "http://xmlns.com/foaf/0.1/birthDay",
              """"2000-10-10T10:10:10.000"^^xsd:dateTime""",
              ""
            ),
            (
              "_:Ana",
              "http://xmlns.com/foaf/0.1/birthDay",
              """"2000-10-10T10:10:10.000"^^xsd:dateTime""",
              ""
            ),
            (
              "_:Henry",
              "http://xmlns.com/foaf/0.1/birthDay",
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect should have length 2
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:Martha"),
            Row("_:Ana")
          )

        }
      }

      "with logical operation GTE" should {

        "execute on simple literal" in {
          import sqlContext.implicits._

          val df: DataFrame = List(
            ("_:a", "http://xmlns.com/foaf/0.1/name", "Anthony", ""),
            ("_:b", "http://xmlns.com/foaf/0.1/name", "Bob", ""),
            ("_:c", "http://xmlns.com/foaf/0.1/name", "Charles", "")
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 2
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:b", "\"Bob\""),
            Row("_:c", "\"Charles\"")
          )
        }

        // TODO: Add support for string syntactic sugar, see: https://lists.w3.org/Archives/Public/public-sparql-dev/2013AprJun/0003.html
        "execute on strings" ignore {
          import sqlContext.implicits._

          val df: DataFrame = List(
            (
              "_:a",
              "http://xmlns.com/foaf/0.1/name",
              "\"Anthony\"^^<http://www.w3.org/2001/XMLSchema#string>",
              ""
            ),
            (
              "_:b",
              "http://xmlns.com/foaf/0.1/name",
              "\"Bob\"^^<http://www.w3.org/2001/XMLSchema#string>",
              ""
            ),
            (
              "_:c",
              "http://xmlns.com/foaf/0.1/name",
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 2
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:b", "\"Bob\"^^xsd:string"),
            Row("_:c", "\"Charles\"^^xsd:string")
          )
        }

        "execute on numbers" in {
          import sqlContext.implicits._

          val df: DataFrame = List(
            ("_:Bob", "http://xmlns.com/foaf/0.1/age", 15, ""),
            ("_:Alice", "http://xmlns.com/foaf/0.1/age", 18, ""),
            ("_:Henry", "http://xmlns.com/foaf/0.1/age", 21, "")
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 2
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:Alice", 18),
            Row("_:Henry", 21)
          )
        }

        "execute on booleans" in {
          import sqlContext.implicits._

          val df: DataFrame = List(
            ("_:Martha", "http://xmlns.com/foaf/0.1/isFemale", true, ""),
            ("_:Henry", "http://xmlns.com/foaf/0.1/isFemale", false, "")
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:Martha", true)
          )
        }

        "execute on dateTimes" in {
          import sqlContext.implicits._

          val df: DataFrame = List(
            ("_:Martha", "http://xmlns.com/foaf/0.1/isFemale", "true", ""),
            ("_:Henry", "http://xmlns.com/foaf/0.1/isFemale", "false", ""),
            ("_:Ana", "http://xmlns.com/foaf/0.1/isFemale", "true", ""),
            (
              "_:Martha",
              "http://xmlns.com/foaf/0.1/birthDay",
              """"2000-10-10T10:10:10.000"^^xsd:dateTime""",
              ""
            ),
            (
              "_:Ana",
              "http://xmlns.com/foaf/0.1/birthDay",
              """"2000-10-10T10:10:10.000"^^xsd:dateTime""",
              ""
            ),
            (
              "_:Henry",
              "http://xmlns.com/foaf/0.1/birthDay",
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect should have length 3
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:Martha"),
            Row("_:Ana"),
            Row("_:Henry")
          )
        }
      }

      "with logical operation LTE" should {

        "execute on simple literal" in {
          import sqlContext.implicits._

          val df: DataFrame = List(
            ("_:a", "http://xmlns.com/foaf/0.1/name", "Anthony", ""),
            ("_:b", "http://xmlns.com/foaf/0.1/name", "Bob", ""),
            ("_:c", "http://xmlns.com/foaf/0.1/name", "Charles", "")
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 2
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:a", "\"Anthony\""),
            Row("_:b", "\"Bob\"")
          )
        }

        // TODO: Add support for string syntactic sugar, see: https://lists.w3.org/Archives/Public/public-sparql-dev/2013AprJun/0003.html
        "execute on strings" ignore {
          import sqlContext.implicits._

          val df: DataFrame = List(
            (
              "_:a",
              "http://xmlns.com/foaf/0.1/name",
              "\"Anthony\"^^<http://www.w3.org/2001/XMLSchema#string>",
              ""
            ),
            (
              "_:b",
              "http://xmlns.com/foaf/0.1/name",
              "\"Bob\"^^<http://www.w3.org/2001/XMLSchema#string>",
              ""
            ),
            (
              "_:c",
              "http://xmlns.com/foaf/0.1/name",
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 2
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:a", "\"Anthony\"^^xsd:string"),
            Row("_:b", "\"Bob\"^^xsd:string")
          )
        }

        "execute on numbers" in {
          import sqlContext.implicits._

          val df: DataFrame = List(
            ("_:Bob", "http://xmlns.com/foaf/0.1/age", 15, ""),
            ("_:Alice", "http://xmlns.com/foaf/0.1/age", 18, ""),
            ("_:Henry", "http://xmlns.com/foaf/0.1/age", 21, "")
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 2
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:Bob", 15),
            Row("_:Alice", 18)
          )
        }

        "execute on booleans" in {
          import sqlContext.implicits._

          val df: DataFrame = List(
            ("_:Martha", "http://xmlns.com/foaf/0.1/isFemale", true, ""),
            ("_:Henry", "http://xmlns.com/foaf/0.1/isFemale", false, "")
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

          val result = Compiler.compile(df, query)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 2
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:Martha", true),
            Row("_:Henry", false)
          )
        }

        // TODO: Implement Date Time support issue
        "execute on dateTimes" in {
          import sqlContext.implicits._

          val df: DataFrame = List(
            ("_:Martha", "http://xmlns.com/foaf/0.1/isFemale", "true", ""),
            ("_:Henry", "http://xmlns.com/foaf/0.1/isFemale", "false", ""),
            ("_:Ana", "http://xmlns.com/foaf/0.1/isFemale", "true", ""),
            (
              "_:Martha",
              "http://xmlns.com/foaf/0.1/birthDay",
              """"2000-10-10T10:10:10.000"^^xsd:dateTime""",
              ""
            ),
            (
              "_:Ana",
              "http://xmlns.com/foaf/0.1/birthDay",
              """"2000-10-10T10:10:10.000"^^xsd:dateTime""",
              ""
            ),
            (
              "_:Henry",
              "http://xmlns.com/foaf/0.1/birthDay",
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

          val result = Compiler.compile(df, query)

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

    "perform query with CONSTRUCT statement" should {

      "execute and apply default ordering CONSTRUCT queries" in {
        import sqlContext.implicits._

        val df: DataFrame = List(
          (
            "http://potato.com/b",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/docSource",
            "http://thesour.ce",
            ""
          ),
          (
            "http://potato.com/c",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/Document",
            ""
          ),
          (
            "http://potato.com/c",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/docSource",
            "http://thesour.ce",
            ""
          ),
          (
            "http://potato.com/d",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/docSource",
            "http://thesour.ce",
            ""
          ),
          (
            "http://potato.com/b",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/Document",
            ""
          ),
          (
            "http://potato.com/d",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/Document",
            ""
          ),
          ("negative", "negative", "negative", "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            PREFIX  dm:   <http://gsk-kg.rdip.gsk.com/dm/1.0/>

            CONSTRUCT {
             ?d a dm:Document .
             ?d dm:docSource ?src .
            }
            WHERE{
             ?d a dm:Document .
             ?d dm:docSource ?src .
            }
            """

        val result = Compiler.compile(df, query)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 6
        result.right.get.collect shouldEqual Array(
          Row(
            "http://potato.com/b",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/docSource",
            "http://thesour.ce"
          ),
          Row(
            "http://potato.com/b",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/Document"
          ),
          Row(
            "http://potato.com/c",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/docSource",
            "http://thesour.ce"
          ),
          Row(
            "http://potato.com/c",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/Document"
          ),
          Row(
            "http://potato.com/d",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/docSource",
            "http://thesour.ce"
          ),
          Row(
            "http://potato.com/d",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/Document"
          )
        )
      }

      "execute and make sense in the LIMIT cause when there's no ORDER BY" in {
        import sqlContext.implicits._

        val df: DataFrame = List(
          (
            "http://potato.com/b",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/Document",
            ""
          ),
          (
            "http://potato.com/c",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/Document",
            ""
          ),
          (
            "http://potato.com/b",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/docSource",
            "http://thesour.ce",
            ""
          ),
          (
            "http://potato.com/d",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/Document",
            ""
          ),
          (
            "http://potato.com/c",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/docSource",
            "http://thesour.ce",
            ""
          ),
          (
            "http://potato.com/d",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/docSource",
            "http://thesour.ce",
            ""
          ),
          ("negative", "negative", "negative", "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
      PREFIX  dm:   <http://gsk-kg.rdip.gsk.com/dm/1.0/>

      CONSTRUCT {
       ?d a dm:Document .
       ?d dm:docSource ?src .
      }
      WHERE{
       ?d a dm:Document .
       ?d dm:docSource ?src .
      }
      LIMIT 1
      """

        val result = Compiler.compile(df, query)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect shouldEqual Array(
          Row(
            "http://potato.com/b",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/docSource",
            "http://thesour.ce"
          ),
          Row(
            "http://potato.com/b",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/Document"
          )
        )
      }

      "execute and work correctly with blank nodes in templates with a single blank label" in {
        import sqlContext.implicits._

        val df: DataFrame = List(
          (
            "http://potato.com/b",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/Document",
            ""
          ),
          (
            "http://potato.com/c",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/Document",
            ""
          ),
          (
            "http://potato.com/b",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/docSource",
            "http://thesour.ce",
            ""
          ),
          (
            "http://potato.com/d",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/Document",
            ""
          ),
          (
            "http://potato.com/c",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/docSource",
            "http://thesour.ce",
            ""
          ),
          (
            "http://potato.com/d",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/docSource",
            "http://thesour.ce",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
      PREFIX  dm:   <http://gsk-kg.rdip.gsk.com/dm/1.0/>

      CONSTRUCT {
       _:asdf a dm:Document .
       _:asdf dm:docSource ?src .
      }
      WHERE{
       ?d a dm:Document .
       ?d dm:docSource ?src .
      }
      """

        val result = Compiler.compile(df, query)

        val arrayResult = result.right.get.collect
        result shouldBe a[Right[_, _]]
        arrayResult should have size 6
        arrayResult.map(_.get(0)).distinct should have size 3
        arrayResult.map(row => (row.get(1), row.get(2))) shouldEqual Array(
          ("http://gsk-kg.rdip.gsk.com/dm/1.0/docSource", "http://thesour.ce"),
          (
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/Document"
          ),
          ("http://gsk-kg.rdip.gsk.com/dm/1.0/docSource", "http://thesour.ce"),
          (
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/Document"
          ),
          ("http://gsk-kg.rdip.gsk.com/dm/1.0/docSource", "http://thesour.ce"),
          (
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/Document"
          )
        )
      }

      "execute and work correctly with blank nodes in templates with more than one blank label" in {
        import sqlContext.implicits._

        val df: DataFrame = List(
          (
            "http://potato.com/b",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/Document",
            ""
          ),
          (
            "http://potato.com/c",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/Document",
            ""
          ),
          (
            "http://potato.com/b",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/docSource",
            "http://thesour.ce",
            ""
          ),
          (
            "http://potato.com/d",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/Document",
            ""
          ),
          (
            "http://potato.com/c",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/docSource",
            "http://thesour.ce",
            ""
          ),
          (
            "http://potato.com/d",
            "http://gsk-kg.rdip.gsk.com/dm/1.0/docSource",
            "http://thesour.ce",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
      PREFIX  dm:   <http://gsk-kg.rdip.gsk.com/dm/1.0/>

      CONSTRUCT {
       _:asdf a dm:Document .
       _:asdf dm:linksInSomeWay _:qwer .
       _:qwer dm:source ?src .
      }
      WHERE{
       ?d a dm:Document .
       ?d dm:docSource ?src .
      }
      """

        val result      = Compiler.compile(df, query)
        val resultDF    = result.right.get
        val arrayResult = resultDF.collect

        result shouldBe a[Right[_, _]]
        arrayResult should have size 9
        arrayResult.map(_.get(0)).distinct should have size 6
      }
    }

    "perform query with OPTIONAL" should {

      "execute and obtain expected results with simple optional" in {

        import sqlContext.implicits._

        val df: DataFrame = List(
          (
            "_:a",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://xmlns.com/foaf/0.1/Person",
            ""
          ),
          ("_:a", "http://xmlns.com/foaf/0.1/name", "Alice", ""),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:alice@example.com",
            ""
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:alice@work.example",
            ""
          ),
          (
            "_:b",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://xmlns.com/foaf/0.1/Person",
            ""
          ),
          ("_:b", "http://xmlns.com/foaf/0.1/name", "Bob", "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |SELECT ?name ?mbox
            |WHERE  { ?x foaf:name  ?name .
            |         OPTIONAL { ?x  foaf:mbox  ?mbox }
            |       }
            |""".stripMargin

        val result = Compiler.compile(df, query)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 3
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"Alice\"", "mailto:alice@example.com"),
          Row("\"Alice\"", "mailto:alice@work.example"),
          Row("\"Bob\"", null)
        )
      }

      "execute and obtain expected results with constraints in optional" in {
        import sqlContext.implicits._

        val df: DataFrame = List(
          (
            "_:book1",
            "http://purl.org/dc/elements/1.1/title",
            "SPARQL Tutorial",
            ""
          ),
          ("_:book1", "http://example.org/ns#price", "42", ""),
          (
            "_:book2",
            "http://purl.org/dc/elements/1.1/title",
            "The Semantic Web",
            ""
          ),
          ("_:book2", "http://example.org/ns#price", "_:23", "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX  dc:  <http://purl.org/dc/elements/1.1/>
            |PREFIX  ns:  <http://example.org/ns#>
            |
            |SELECT  ?title ?price
            |WHERE   {
            |   ?x dc:title ?title .
            |   OPTIONAL {
            |     ?x ns:price ?price
            |     FILTER(isBlank(?price))
            |   }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"SPARQL Tutorial\"", null),
          Row("\"The Semantic Web\"", "_:23")
        )
      }

      "execute and obtain expected results with multiple optionals" in {
        import sqlContext.implicits._

        val df: DataFrame = List(
          ("_:a", "http://xmlns.com/foaf/0.1/name", "Alice", ""),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/homepage",
            "http://work.example.org/alice/",
            ""
          ),
          ("_:b", "http://xmlns.com/foaf/0.1/name", "Bob", ""),
          (
            "_:b",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:bob@work.example",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?name ?mbox ?hpage
            |WHERE  {
            |   ?x foaf:name  ?name .
            |   OPTIONAL { ?x foaf:mbox ?mbox } .
            |   OPTIONAL { ?x foaf:homepage ?hpage }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"Alice\"", null, "http://work.example.org/alice/"),
          Row("\"Bob\"", "mailto:bob@work.example", null)
        )
      }
    }

    "perform query with !ISBLANK function" should {

      "execute and obtain expected results" in {
        import sqlContext.implicits._

        val df: DataFrame = List(
          ("_:a", "http://xmlns.com/foaf/0.1/name", "Alice", ""),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:alice@work.example",
            ""
          ),
          ("_:b", "http://xmlns.com/foaf/0.1/name", "_:bob", ""),
          (
            "_:b",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:bob@work.example",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?name ?mbox
            |WHERE {
            |   ?x foaf:name ?name ;
            |      foaf:mbox  ?mbox .
            |   FILTER (!isBlank(?name))
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect shouldEqual Array(
          Row("\"Alice\"", "mailto:alice@work.example")
        )
      }
    }

    "perform query with DISTINCT modifier" should {

      "execute and obtain expected results" in {
        import sqlContext.implicits._

        val df: DataFrame = List(
          ("_:a", "http://xmlns.com/foaf/0.1/name", "Alice", ""),
          ("_:b", "http://xmlns.com/foaf/0.1/name", "Bob", ""),
          ("_:c", "http://xmlns.com/foaf/0.1/name", "Alice", "")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |
            |SELECT DISTINCT ?name
            |WHERE  {
            |   ?x foaf:name  ?name
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"Alice\""),
          Row("\"Bob\"")
        )
      }
    }

    "perform query with GRAPH expression" should {

      "execute and obtain expected results with one graph specified" in {
        import sqlContext.implicits._

        val df: DataFrame = List(
          // Default graph
          (
            "http://example.org/bob",
            "http://purl.org/dc/elements/1.1/publisher",
            "Bob Hacker",
            ""
          ),
          (
            "http://example.org/alice",
            "http://purl.org/dc/elements/1.1/publisher",
            "Alice Hacker",
            ""
          ),
          // Alice graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Alice",
            "http://example.org/alice"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:alice@work.example.org",
            "http://example.org/alice"
          ),
          // Bob graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Bob",
            "http://example.org/bob"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:bob@oldcorp.example.org",
            "http://example.org/bob"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX dc: <http://purl.org/dc/elements/1.1/>
            |PREFIX ex: <http://example.org/>
            |
            |SELECT ?mbox
            |FROM <http://example.org/dft.ttl>
            |FROM NAMED <http://example.org/alice>
            |FROM NAMED <http://example.org/bob>
            |WHERE
            |{
            |   GRAPH ex:alice { ?x foaf:mbox ?mbox }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("mailto:alice@work.example.org")
        )
      }

      "execute and obtain expected results with multiple graphs specified" in {
        import sqlContext.implicits._

        val df: DataFrame = List(
          // Default graph
          (
            "http://example.org/bob",
            "http://purl.org/dc/elements/1.1/publisher",
            "Bob Hacker",
            ""
          ),
          (
            "http://example.org/alice",
            "http://purl.org/dc/elements/1.1/publisher",
            "Alice Hacker",
            ""
          ),
          (
            "http://example.org/charles",
            "http://purl.org/dc/elements/1.1/publisher",
            "Charles Hacker",
            ""
          ),
          // Alice graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Alice",
            "http://example.org/alice"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:alice@work.example.org",
            "http://example.org/alice"
          ),
          // Bob graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Bob",
            "http://example.org/bob"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:bob@oldcorp.example.org",
            "http://example.org/bob"
          ),
          // Charles graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Charles",
            "http://example.org/charles"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:charles@work.example.org",
            "http://example.org/charles"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX dc: <http://purl.org/dc/elements/1.1/>
            |PREFIX ex: <http://example.org/>
            |
            |SELECT ?mbox
            |FROM <http://example.org/dft.ttl>
            |FROM NAMED <http://example.org/alice>
            |FROM NAMED <http://example.org/bob>
            |FROM NAMED <http://example.org/charles>
            |WHERE
            |{
            |   { GRAPH ex:alice { ?x foaf:mbox ?mbox } }
            |   UNION
            |   { GRAPH ex:bob { ?y foaf:mbox ?mbox } }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("mailto:alice@work.example.org"),
          Row("mailto:bob@oldcorp.example.org")
        )
      }

      // TODO: Un-ignore when JOIN implemented and named graphs support for variables
      "execute and obtain expected results when referenced graph is a variable instead of a specified graph" ignore {
        import sqlContext.implicits._

        val df: DataFrame = List(
          // Default graph
          (
            "http://example.org/bob",
            "http://purl.org/dc/elements/1.1/publisher",
            "Bob Hacker",
            ""
          ),
          (
            "http://example.org/alice",
            "http://purl.org/dc/elements/1.1/publisher",
            "Alice Hacker",
            ""
          ),
          (
            "http://example.org/charles",
            "http://purl.org/dc/elements/1.1/publisher",
            "Charles Hacker",
            ""
          ),
          // Alice graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Alice",
            "http://example.org/alice"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:alice@work.example.org",
            "http://example.org/alice"
          ),
          // Bob graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Bob",
            "http://example.org/bob"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:bob@oldcorp.example.org",
            "http://example.org/bob"
          ),
          // Charles graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Charles",
            "http://example.org/charles"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:charles@work.example.org",
            "http://example.org/charles"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX dc: <http://purl.org/dc/elements/1.1/>
            |PREFIX ex: <http://example.org/>
            |
            |SELECT ?who ?g ?mbox
            |FROM <http://example.org/dft.ttl>
            |FROM NAMED <http://example.org/alice>
            |FROM NAMED <http://example.org/bob>
            |FROM NAMED <http://example.org/charles>
            |WHERE
            |{
            |   ?g dc:publisher ?who .
            |   GRAPH ?g { ?x foaf:mbox ?mbox }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 3
        result.right.get.collect.toSet shouldEqual Set(
          Row(
            "Alice Hacker",
            "http://example.org/alice",
            "mailto:alice@work.example.org"
          ),
          Row(
            "Bob Hacker",
            "http://example.org/bob",
            "mailto:bob@oldcorp.example.org"
          ),
          Row(
            "Charles Hacker",
            "http://example.org/charles",
            "mailto:charles@work.example.org"
          )
        )
      }
    }
  }

  private def readNTtoDF(path: String) = {
    import sqlContext.implicits._
    import scala.collection.JavaConverters._

    val filename                            = s"modules/engine/src/test/resources/$path"
    val inputStream: CollectorStreamTriples = new CollectorStreamTriples()
    RDFParser.source(filename).parse(inputStream)

    inputStream
      .getCollected()
      .asScala
      .toList
      .map(triple =>
        (
          triple.getSubject().toString(),
          triple.getPredicate().toString(),
          triple.getObject().toString(),
          ""
        )
      )
      .toDF("s", "p", "o", "g")

  }

}
