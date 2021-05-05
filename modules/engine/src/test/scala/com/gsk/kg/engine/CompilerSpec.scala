package com.gsk.kg.engine

import org.apache.jena.riot.RDFParser
import org.apache.jena.riot.lang.CollectorStreamTriples

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.sparqlparser.EngineError
import com.gsk.kg.sparqlparser.EngineError.ParsingError
import com.gsk.kg.sparqlparser.TestConfig

import java.io.ByteArrayOutputStream

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CompilerSpec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
    with TestConfig {

  import sqlContext.implicits._

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

      val result = Compiler.compile(df, query, config)

      result shouldBe a[Right[_, _]]
      result.right.get.collect().length shouldEqual 4
      result.right.get.collect().toSet shouldEqual Set(
        Row("\"5.88\"^^<http://www.w3.org/2001/XMLSchema#float>"),
        Row("\"0.22\"^^xsd:float"),
        Row("\"foo\"^^xsd:string"),
        Row("\"true\"^^xsd:boolean")
      )
    }

    "remove question marks from variable columns when flag setup" in {

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
          |SELECT  ?s ?o
          |WHERE   { ?s foaf:name ?o }
          |""".stripMargin

      val result = Compiler.compile(
        df,
        query,
        config.copy(stripQuestionMarksOnOutput = true)
      )

      val expectedOut =
        """+------+---------+
          ||     s|        o|
          |+------+---------+
          ||"team"|"Anthony"|
          ||"team"| "Perico"|
          ||"team"|  "Henry"|
          |+------+---------+
          |
          |""".stripMargin

      result shouldBe a[Right[_, _]]

      val outCapture = new ByteArrayOutputStream
      Console.withOut(outCapture) {
        result.right.get.show()
      }

      outCapture.toString shouldEqual expectedOut
    }

    /** TODO(pepegar): In order to make this test pass we need the
      * results to be RDF compliant (mainly, wrapping values correctly)
      */
    "query a real DF with a real query" ignore {
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

      val inputDF = readNTtoDF("fixtures/reference-q1-input.nt")
      val outputDF =
        RdfFormatter.formatDataFrame(
          readNTtoDF("fixtures/reference-q1-output.nt"),
          config
        )

      val result = Compiler.compile(inputDF, query, config)

      result shouldBe a[Right[_, _]]
      result.right.get.collect.toSet shouldEqual outputDF
        .drop("g")
        .collect()
        .toSet
    }

    "perform query with BGPs" should {

      "will execute operations in the dataframe" in {

        val df = dfList.toDF("s", "p", "o", "g")
        val query =
          """
            SELECT
              ?s ?p ?o
            WHERE {
              ?s ?p ?o .
            }
            """

        Compiler
          .compile(df, query, config)
          .right
          .get
          .collect() shouldEqual Array(
          Row(
            "\"test\"",
            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
            "<http://id.gsk.com/dm/1.0/Document>"
          ),
          Row("\"test\"", "<http://id.gsk.com/dm/1.0/docSource>", "\"source\"")
        )
      }

      "will execute with two dependent BGPs" in {

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

        Compiler
          .compile(df, query, config)
          .right
          .get
          .collect() shouldEqual Array(
          Row("\"test\"", "\"source\"")
        )
      }
    }

    "perform query with UNION statement" should {

      "execute with the same bindings" in {

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

        Compiler
          .compile(df, query, config)
          .right
          .get
          .collect() shouldEqual Array(
          Row("\"test\"", "<http://id.gsk.com/dm/1.0/Document>"),
          Row("\"test\"", "\"source\"")
        )
      }

      "execute with different bindings" in {

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

        Compiler
          .compile(df, query, config)
          .right
          .get
          .collect() shouldEqual Array(
          Row("\"test\"", "<http://id.gsk.com/dm/1.0/Document>", null, null),
          Row(null, null, "\"test\"", "\"source\"")
        )
      }
    }

    "perform with CONSTRUCT statement" should {

      "execute with a single triple pattern" in {

        val df: DataFrame = dfList.toDF("s", "p", "o", "g")

        val query =
          """
        CONSTRUCT {
          ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o
        } WHERE {
          ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o
        }
        """

        Compiler
          .compile(df, query, config)
          .right
          .get
          .collect() shouldEqual Array(
          Row(
            "\"test\"",
            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
            "<http://id.gsk.com/dm/1.0/Document>"
          )
        )
      }

      "execute with more than one triple pattern" in {

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
            |CONSTRUCT {
            |  ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o .
            |  ?s2 <http://id.gsk.com/dm/1.0/docSource> ?o2
            |} WHERE {
            |  ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o .
            |  ?s2 <http://id.gsk.com/dm/1.0/docSource> ?o2
            |}
            |""".stripMargin

        val result =
          Compiler.compile(df, query, config).right.get.collect().toSet
        result shouldEqual Set(
          Row(
            "\"doesmatch\"",
            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
            "<http://id.gsk.com/dm/1.0/Document>"
          ),
          Row(
            "\"doesmatchaswell\"",
            "<http://id.gsk.com/dm/1.0/docSource>",
            "\"potato\""
          ),
          Row(
            "\"test\"",
            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
            "<http://id.gsk.com/dm/1.0/Document>"
          ),
          Row(
            "\"test\"",
            "<http://id.gsk.com/dm/1.0/docSource>",
            "\"source\""
          )
        )
      }

      "execute with more than one triple pattern with common bindings" in {

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

        Compiler
          .compile(df, query, config)
          .right
          .get
          .collect()
          .toSet shouldEqual Set(
          Row(
            "\"test\"",
            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
            "<http://id.gsk.com/dm/1.0/Document>"
          ),
          Row(
            "\"test\"",
            "<http://id.gsk.com/dm/1.0/docSource>",
            "\"source\""
          )
        )
      }
    }

    "perform query with LIMIT modifier" should {

      "execute with limit greater than 0" in {

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

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"Anthony\""),
          Row("\"Perico\"")
        )
      }

      "execute with limit equal to 0 and obtain no results" in {

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

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 0
        result.right.get.collect.toSet shouldEqual Set.empty
      }

      "execute with limit greater than Java MAX INTEGER and obtain an error" in {

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

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Left[_, _]]
        result.left.get shouldEqual EngineError.NumericTypesDoNotMatch(
          "2147483648 to big to be converted to an Int"
        )
      }
    }

    "perform query with OFFSET modifier" should {

      "execute with offset greater than 0 and obtain a non empty set" in {

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

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"Perico\""),
          Row("\"Henry\"")
        )
      }

      "execute with offset equal to 0 and obtain same elements as the original set" in {

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

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 3
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"Anthony\""),
          Row("\"Perico\""),
          Row("\"Henry\"")
        )
      }

      "execute with offset greater than the number of elements of the dataframe and obtain an empty set" in {

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

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 0
        result.right.get.collect.toSet shouldEqual Set.empty
      }
    }

    "perform query with Blank nodes" should {

      "execute and obtain expected results" in {

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

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"nodeA\"", "\"otherThingy\"")
        )
      }
    }

    "perform query with REPLACE function" should {

      "execute and obtain expected results without flags" in {

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

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 4
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"abcd\"", "\"aZcd\""),
          Row("\"abaB\"", "\"aZaB\""),
          Row("\"bbBB\"", "\"ZZBB\""),
          Row("\"aaaa\"", "\"aaaa\"")
        )
      }

      "execute and obtain expected results with flags" in {

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
            |  BIND(REPLACE(?lit, "b", "Z", "i") AS ?lit2)
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 4
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"abcd\"", "\"aZcd\""),
          Row("\"abaB\"", "\"aZaZ\""),
          Row("\"bbBB\"", "\"ZZZZ\""),
          Row("\"aaaa\"", "\"aaaa\"")
        )
      }

      // TODO: Capture Spark exceptions on the Left projection once the query trigger the Spark action
      // this can be done probably at the Engine level
      "execute and obtain an expected error, " +
        "because the pattern matches the zero-length string" ignore {

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

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Left[_, _]]
          result.left.get shouldEqual EngineError.FunctionError(
            s"Error on REPLACE function: No group 1"
          )
        }
    }

    "perform query with ISBLANK function" should {

      "execute and obtain expected results" in {

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

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"Bob\"", "\"Smith\"")
        )
      }
    }

    "perform query with !ISBLANK function" should {

      "execute and obtain expected results" in {

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

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect shouldEqual Array(
          Row("\"Alice\"", "<mailto:alice@work.example>")
        )
      }
    }

    "perform query with FILTER modifier" when {

      "single condition" should {

        "execute and obtain expected results" in {

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

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(Row("\"Blank\""))
        }

        "execute and obtain expected results when condition has embedded functions" in {

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

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(Row("\"Blank\""))
        }

        "execute and obtain expected results when double filter" in {

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

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:", "\"Henry\"")
          )
        }

        "execute and obtain expected results when there is OR condition" in {

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

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row(":_a", "\"Henry\"^^xsd:string")
          )
        }

        "execute on numbers" in {

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

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:Henry", "21")
          )
        }

        "execute on booleans" in {

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

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:Martha", "true")
          )
        }

        "execute on datetimes" in {

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

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row(":_b", "\"Perico\"^^xsd:string")
          )
        }

        "execute on numbers" in {

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

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:Perico", "15")
          )
        }

        "execute on booleans" in {

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

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:Henry", "false")
          )
        }

        "execute on dateTimes" in {

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

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:b", "\"Charles\"^^xsd:string")
          )
        }

        "execute on numbers" in {

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

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:Henry", "21")
          )
        }

        "execute on booleans" in {

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

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 0
          result.right.get.collect.toSet shouldEqual Set()
        }

        // TODO: Implement Date Time support issue
        "execute on dateTimes" in {

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

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:a", "\"Anthony\"^^xsd:string")
          )
        }

        "execute on numbers" in {

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

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:Bob", "15")
          )
        }

        "execute on booleans" in {

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

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:Martha", "true")
          )
        }

        "execute on dateTimes" in {

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

    "perform query with CONSTRUCT statement" should {

      "execute and apply default ordering CONSTRUCT queries" in {

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

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 6
        result.right.get.collect shouldEqual Array(
          Row(
            "<http://potato.com/b>",
            "<http://gsk-kg.rdip.gsk.com/dm/1.0/docSource>",
            "<http://thesour.ce>"
          ),
          Row(
            "<http://potato.com/b>",
            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
            "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>"
          ),
          Row(
            "<http://potato.com/c>",
            "<http://gsk-kg.rdip.gsk.com/dm/1.0/docSource>",
            "<http://thesour.ce>"
          ),
          Row(
            "<http://potato.com/c>",
            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
            "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>"
          ),
          Row(
            "<http://potato.com/d>",
            "<http://gsk-kg.rdip.gsk.com/dm/1.0/docSource>",
            "<http://thesour.ce>"
          ),
          Row(
            "<http://potato.com/d>",
            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
            "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>"
          )
        )
      }

      "execute and make sense in the LIMIT cause when there's no ORDER BY" in {

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

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect shouldEqual Array(
          Row(
            "<http://potato.com/b>",
            "<http://gsk-kg.rdip.gsk.com/dm/1.0/docSource>",
            "<http://thesour.ce>"
          ),
          Row(
            "<http://potato.com/b>",
            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
            "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>"
          )
        )
      }

      "execute and work correctly with blank nodes in templates with a single blank label" in {

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

        val result = Compiler.compile(df, query, config)

        val arrayResult = result.right.get.collect
        result shouldBe a[Right[_, _]]
        arrayResult should have size 6
        arrayResult.map(_.get(0)).distinct should have size 3
        arrayResult.map(row => (row.get(1), row.get(2))) shouldEqual Array(
          (
            "<http://gsk-kg.rdip.gsk.com/dm/1.0/docSource>",
            "<http://thesour.ce>"
          ),
          (
            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
            "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>"
          ),
          (
            "<http://gsk-kg.rdip.gsk.com/dm/1.0/docSource>",
            "<http://thesour.ce>"
          ),
          (
            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
            "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>"
          ),
          (
            "<http://gsk-kg.rdip.gsk.com/dm/1.0/docSource>",
            "<http://thesour.ce>"
          ),
          (
            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
            "<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>"
          )
        )
      }

      "execute and work correctly with blank nodes in templates with more than one blank label" in {

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

        val result      = Compiler.compile(df, query, config)
        val resultDF    = result.right.get
        val arrayResult = resultDF.collect

        result shouldBe a[Right[_, _]]
        arrayResult should have size 9
        arrayResult.map(_.get(0)).distinct should have size 6
      }
    }

    "perform query with OPTIONAL" should {

      "execute and obtain expected results with simple optional" in {

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

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 3
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"Alice\"", "<mailto:alice@example.com>"),
          Row("\"Alice\"", "<mailto:alice@work.example>"),
          Row("\"Bob\"", null)
        )
      }

      "execute and obtain expected results with constraints in optional" in {

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

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"SPARQL Tutorial\"", null),
          Row("\"The Semantic Web\"", "_:23")
        )
      }

      "execute and obtain expected results with multiple optionals" in {

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

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"Alice\"", null, "<http://work.example.org/alice/>"),
          Row("\"Bob\"", "<mailto:bob@work.example>", null)
        )
      }
    }

    "perform query with DISTINCT modifier" should {

      "execute and obtain expected results" in {

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

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"Alice\""),
          Row("\"Bob\"")
        )
      }
    }

    "perform query with GRAPH expression on default and named graphs" when {

      "simple specific graph" should {

        "execute and obtain expected results with one graph specified" in {

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

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("<mailto:alice@work.example.org>")
          )
        }

        "execute and obtain expected results with one graph specified and UNION inside GRAPH statement" in {

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
              |SELECT ?mbox ?name
              |FROM <http://example.org/dft.ttl>
              |FROM NAMED <http://example.org/alice>
              |FROM NAMED <http://example.org/bob>
              |WHERE
              |{
              |   GRAPH ex:alice {
              |     { ?x foaf:mbox ?mbox }
              |     UNION
              |     { ?x foaf:name ?name }
              |   }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 2
          result.right.get.collect.toSet shouldEqual Set(
            Row("<mailto:alice@work.example.org>", null),
            Row(null, "\"Alice\"")
          )
        }

        "execute and obtain expected results with one graph specified and JOIN inside GRAPH statement" in {

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
              |SELECT ?mbox ?name
              |FROM <http://example.org/dft.ttl>
              |FROM NAMED <http://example.org/alice>
              |FROM NAMED <http://example.org/bob>
              |WHERE
              |{
              |   GRAPH ex:alice {
              |     ?x foaf:mbox ?mbox .
              |     ?x foaf:name ?name .
              |   }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("<mailto:alice@work.example.org>", "\"Alice\"")
          )
        }

        "execute and obtain expected results with one graph specified and OPTIONAL inside GRAPH statement" in {}
      }

      "multiple specific named graphs" should {

        "execute and obtain expected results when UNION with common variable bindings" in {

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
              |SELECT ?x ?y ?mbox
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

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 2
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:a", null, "<mailto:alice@work.example.org>"),
            Row(null, "_:a", "<mailto:bob@oldcorp.example.org>")
          )
        }

        "execute and obtain expected results when JOIN with common variable bindings" in {

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
              "mailto:bob@work.example.org",
              "http://example.org/bob"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
              |PREFIX dc: <http://purl.org/dc/elements/1.1/>
              |PREFIX ex: <http://example.org/>
              |
              |SELECT ?x ?mbox ?name
              |FROM <http://example.org/dft.ttl>
              |FROM NAMED <http://example.org/alice>
              |FROM NAMED <http://example.org/bob>
              |WHERE
              |{
              |   GRAPH ex:alice { ?x foaf:mbox ?mbox . }
              |   GRAPH ex:bob { ?x foaf:name ?name . }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:a", "<mailto:alice@work.example.org>", "\"Bob\"")
          )
        }

        "execute and obtain expected results when JOIN with no common variable bindings" in {

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
              "_:b",
              "http://xmlns.com/foaf/0.1/name",
              "Bob",
              "http://example.org/bob"
            ),
            (
              "_:b",
              "http://xmlns.com/foaf/0.1/mbox",
              "mailto:bob@work.example.org",
              "http://example.org/bob"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
              |PREFIX dc: <http://purl.org/dc/elements/1.1/>
              |PREFIX ex: <http://example.org/>
              |
              |SELECT ?x ?y
              |FROM <http://example.org/dft.ttl>
              |FROM NAMED <http://example.org/alice>
              |FROM NAMED <http://example.org/bob>
              |WHERE
              |{
              |   GRAPH ex:alice { ?x foaf:mbox <mailto:alice@work.example.org> . }
              |   GRAPH ex:bob { ?y foaf:mbox <mailto:bob@work.example.org> . }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(Row("_:a", "_:b"))
        }

        "execute and obtain expected results when OPTIONAL with common variable bindings" in {

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
              |SELECT ?x ?mbox ?name
              |FROM <http://example.org/dft.ttl>
              |FROM NAMED <http://example.org/alice>
              |FROM NAMED <http://example.org/bob>
              |WHERE  {
              |  GRAPH ex:alice { ?x foaf:name ?name . }
              |  OPTIONAL {
              |    GRAPH ex:bob { ?x foaf:mbox ?mbox }
              |  }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:a", "<mailto:bob@oldcorp.example.org>", "\"Alice\"")
          )
        }

        "execute and obtain expected results when OPTIONAL with no common variable bindings" in {

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
              |SELECT ?x ?y ?mbox ?name
              |FROM <http://example.org/dft.ttl>
              |FROM NAMED <http://example.org/alice>
              |FROM NAMED <http://example.org/bob>
              |WHERE  {
              |  GRAPH ex:alice { ?x foaf:name ?name . }
              |  OPTIONAL {
              |    GRAPH ex:bob { ?y foaf:mbox ?mbox }
              |  }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:a", "_:a", "<mailto:bob@oldcorp.example.org>", "\"Alice\"")
          )
        }
      }

      "mixing default and named graph" should {

        "execute and obtain expected results when UNION with common variable bindings" in {

          val df: DataFrame = List(
            // Default graph - Alice
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
            // Named graph - Bob
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
              |PREFIX ex: <http://example.org/>
              |
              |SELECT ?x ?mbox ?name
              |FROM <http://example.org/alice>
              |FROM NAMED <http://example.org/bob>
              |WHERE
              |{
              |   { ?x foaf:name ?name }
              |   UNION
              |   { GRAPH ex:bob { ?x foaf:mbox ?mbox } }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 2
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:a", null, "\"Alice\""),
            Row("_:a", "<mailto:bob@oldcorp.example.org>", null)
          )
        }

        "execute and obtain expected results when JOIN with common variable bindings" in {

          val df: DataFrame = List(
            // Default graph - Alice
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
            // Default graph - Charles
            (
              "_:a",
              "http://xmlns.com/foaf/0.1/name",
              "Charles",
              "http://example.org/charles"
            ),
            (
              "_:a",
              "http://xmlns.com/foaf/0.1/mbox",
              "mailto:charles@oldcorp.example.org",
              "http://example.org/charles"
            ),
            // Named graph - Bob
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
              |PREFIX ex: <http://example.org/>
              |
              |SELECT ?x ?mbox ?name
              |FROM <http://example.org/alice>
              |FROM <http://example.org/charles>
              |FROM NAMED <http://example.org/bob>
              |WHERE
              |{
              |   ?x foaf:name ?name .
              |   GRAPH ex:bob { ?x foaf:mbox ?mbox }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 2
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:a", "<mailto:bob@oldcorp.example.org>", "\"Alice\""),
            Row("_:a", "<mailto:bob@oldcorp.example.org>", "\"Charles\"")
          )
        }

        "execute and obtain expected results when JOIN with no common variable bindings" in {

          val df: DataFrame = List(
            // Default graph - Alice
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
            // Default graph - Charles
            (
              "_:a",
              "http://xmlns.com/foaf/0.1/name",
              "Charles",
              "http://example.org/charles"
            ),
            (
              "_:a",
              "http://xmlns.com/foaf/0.1/mbox",
              "mailto:charles@oldcorp.example.org",
              "http://example.org/charles"
            ),
            // Named graph - Bob
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
              |PREFIX ex: <http://example.org/>
              |
              |SELECT ?x ?y ?mbox ?name
              |FROM <http://example.org/alice>
              |FROM <http://example.org/charles>
              |FROM NAMED <http://example.org/bob>
              |WHERE
              |{
              |   ?x foaf:name ?name .
              |   GRAPH ex:bob { ?y foaf:mbox ?mbox }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 2
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:a", "_:a", "<mailto:bob@oldcorp.example.org>", "\"Alice\""),
            Row("_:a", "_:a", "<mailto:bob@oldcorp.example.org>", "\"Charles\"")
          )
        }

        "execute and obtain expected results when OPTIONAL with common variable bindings" in {

          val df: DataFrame = List(
            // Default graph - Alice
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
            // Default graph - Charles
            (
              "_:a",
              "http://xmlns.com/foaf/0.1/name",
              "Charles",
              "http://example.org/charles"
            ),
            (
              "_:a",
              "http://xmlns.com/foaf/0.1/mbox",
              "mailto:charles@oldcorp.example.org",
              "http://example.org/charles"
            ),
            // Named graph - Bob
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
              |PREFIX ex: <http://example.org/>
              |
              |SELECT ?x ?mbox ?name
              |FROM <http://example.org/alice>
              |FROM <http://example.org/charles>
              |FROM NAMED <http://example.org/bob>
              |WHERE
              |{
              |   ?x foaf:name ?name .
              |   OPTIONAL {
              |     GRAPH ex:bob { ?x foaf:mbox ?mbox }
              |   }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 2
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:a", "<mailto:bob@oldcorp.example.org>", "\"Alice\""),
            Row("_:a", "<mailto:bob@oldcorp.example.org>", "\"Charles\"")
          )
        }

        "execute and obtain expected results when OPTIONAL with no common variable bindings" in {

          val df: DataFrame = List(
            // Default graph - Alice
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
            // Default graph - Charles
            (
              "_:a",
              "http://xmlns.com/foaf/0.1/name",
              "Charles",
              "http://example.org/charles"
            ),
            (
              "_:a",
              "http://xmlns.com/foaf/0.1/mbox",
              "mailto:charles@oldcorp.example.org",
              "http://example.org/charles"
            ),
            // Named graph - Bob
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
              |PREFIX ex: <http://example.org/>
              |
              |SELECT ?x ?y ?mbox ?name
              |FROM <http://example.org/alice>
              |FROM <http://example.org/charles>
              |FROM NAMED <http://example.org/bob>
              |WHERE
              |{
              |   ?x foaf:name ?name .
              |   OPTIONAL {
              |     GRAPH ex:bob { ?y foaf:mbox ?mbox }
              |   }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 2
          result.right.get.collect.toSet shouldEqual Set(
            Row("_:a", "_:a", "<mailto:bob@oldcorp.example.org>", "\"Alice\""),
            Row("_:a", "_:a", "<mailto:bob@oldcorp.example.org>", "\"Charles\"")
          )
        }
      }

      "graph variables" should {

        "execute and obtain expected results when GRAPH with variable and same variable binding on default graph BGP" in {
          import sqlContext.implicits._

          val df: DataFrame = List(
            // Default graph
            (
              "http://example.org/bob",
              "http://purl.org/dc/elements/1.1/publisher",
              "Bob Hacker",
              "http://example.org/dft.ttl"
            ),
            (
              "http://example.org/alice",
              "http://purl.org/dc/elements/1.1/publisher",
              "Alice Hacker",
              "http://example.org/dft.ttl"
            ),
            (
              "http://example.org/charles",
              "http://purl.org/dc/elements/1.1/publisher",
              "Charles Hacker",
              "http://example.org/dft.ttl"
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

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 3
          result.right.get.collect.toSet shouldEqual Set(
            Row(
              "\"Alice Hacker\"",
              "<http://example.org/alice>",
              "<mailto:alice@work.example.org>"
            ),
            Row(
              "\"Bob Hacker\"",
              "<http://example.org/bob>",
              "<mailto:bob@oldcorp.example.org>"
            ),
            Row(
              "\"Charles Hacker\"",
              "<http://example.org/charles>",
              "<mailto:charles@work.example.org>"
            )
          )
        }

        "execute and obtain expected results when GRAPH with variable, one named graph and common variables" in {

          val df: DataFrame = List(
            // Default graph
            (
              "http://example.org/bob",
              "http://purl.org/dc/elements/1.1/publisher",
              "Bob Hacker",
              "http://example.org/dft.ttl"
            ),
            (
              "http://example.org/alice",
              "http://purl.org/dc/elements/1.1/publisher",
              "Alice Hacker",
              "http://example.org/dft.ttl"
            ),
            (
              "http://example.org/charles",
              "http://purl.org/dc/elements/1.1/publisher",
              "Charles Hacker",
              "http://example.org/dft.ttl"
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
              |PREFIX ex: <http://example.org/>
              |
              |SELECT ?x ?mbox ?name ?g
              |FROM <http://example.org/alice>
              |FROM <http://example.org/charles>
              |FROM NAMED <http://example.org/bob>
              |WHERE
              |{
              |   ?x foaf:name ?name .
              |   GRAPH ?g { ?x foaf:mbox ?mbox }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 2
          result.right.get.collect.toSet shouldEqual Set(
            Row(
              "_:a",
              "<mailto:bob@oldcorp.example.org>",
              "\"Alice\"",
              "<http://example.org/bob>"
            ),
            Row(
              "_:a",
              "<mailto:bob@oldcorp.example.org>",
              "\"Charles\"",
              "<http://example.org/bob>"
            )
          )
        }

        "execute and obtain expected results when GRAPH with variable, two named graph and common variables" in {

          val df: DataFrame = List(
            // Default graph
            (
              "http://example.org/bob",
              "http://purl.org/dc/elements/1.1/publisher",
              "Bob Hacker",
              "http://example.org/dft.ttl"
            ),
            (
              "http://example.org/alice",
              "http://purl.org/dc/elements/1.1/publisher",
              "Alice Hacker",
              "http://example.org/dft.ttl"
            ),
            (
              "http://example.org/charles",
              "http://purl.org/dc/elements/1.1/publisher",
              "Charles Hacker",
              "http://example.org/dft.ttl"
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
              |PREFIX ex: <http://example.org/>
              |
              |SELECT ?x ?mbox ?name ?g
              |FROM <http://example.org/alice>
              |FROM NAMED <http://example.org/charles>
              |FROM NAMED <http://example.org/bob>
              |WHERE
              |{
              |   ?x foaf:name ?name .
              |   GRAPH ?g { ?x foaf:mbox ?mbox }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 2
          result.right.get.collect.toSet shouldEqual Set(
            Row(
              "_:a",
              "<mailto:charles@work.example.org>",
              "\"Alice\"",
              "<http://example.org/charles>"
            ),
            Row(
              "_:a",
              "<mailto:bob@oldcorp.example.org>",
              "\"Alice\"",
              "<http://example.org/bob>"
            )
          )
        }

        "execute and obtain expected results when GRAPH with variable, two named graph and no common variables" in {

          val df: DataFrame = List(
            // Default graph
            (
              "http://example.org/bob",
              "http://purl.org/dc/elements/1.1/publisher",
              "Bob Hacker",
              "http://example.org/dft.ttl"
            ),
            (
              "http://example.org/alice",
              "http://purl.org/dc/elements/1.1/publisher",
              "Alice Hacker",
              "http://example.org/dft.ttl"
            ),
            (
              "http://example.org/charles",
              "http://purl.org/dc/elements/1.1/publisher",
              "Charles Hacker",
              "http://example.org/dft.ttl"
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
              |PREFIX ex: <http://example.org/>
              |
              |SELECT ?x ?y ?mbox ?name ?g
              |FROM <http://example.org/alice>
              |FROM NAMED <http://example.org/charles>
              |FROM NAMED <http://example.org/bob>
              |WHERE
              |{
              |   ?x foaf:name ?name .
              |   GRAPH ?g { ?y foaf:mbox ?mbox }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 2
          result.right.get.collect.toSet shouldEqual Set(
            Row(
              "_:a",
              "_:a",
              "<mailto:charles@work.example.org>",
              "\"Alice\"",
              "<http://example.org/charles>"
            ),
            Row(
              "_:a",
              "_:a",
              "<mailto:bob@oldcorp.example.org>",
              "\"Alice\"",
              "<http://example.org/bob>"
            )
          )
        }

        "execute and obtain expected results when GRAPH with variable over mixed named and default graphs with JOIN" in {

          import sqlContext.implicits._

          val df: DataFrame = List(
            // Default graph
            (
              "http://example.org/bob",
              "http://purl.org/dc/elements/1.1/publisher",
              "Bob Hacker",
              "http://example.org/dft.ttl"
            ),
            (
              "http://example.org/alice",
              "http://purl.org/dc/elements/1.1/publisher",
              "Alice Hacker",
              "http://example.org/dft.ttl"
            ),
            (
              "http://example.org/charles",
              "http://purl.org/dc/elements/1.1/publisher",
              "Charles Hacker",
              "http://example.org/dft.ttl"
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
              |PREFIX ex: <http://example.org/>
              |
              |SELECT ?x ?mbox ?name ?g
              |FROM <http://example.org/alice>
              |FROM <http://example.org/charles>
              |FROM NAMED <http://example.org/bob>
              |WHERE
              |{
              |   GRAPH ?g {
              |     ?x foaf:name ?name .
              |     GRAPH ex:bob { ?x foaf:mbox ?mbox }
              |   }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 1
          result.right.get.collect.toSet shouldEqual Set(
            Row(
              "_:a",
              "<mailto:bob@oldcorp.example.org>",
              "\"Bob\"",
              "<http://example.org/bob>"
            )
          )
        }
      }

      "case of study graph queries" should {

        // TODO: This query seems not to be consistent with Jena, shall be analyzed as a case of study
        "execute and obtain expected results when GRAPH with variable over mixed named and default graphs with UNION" in {

          import sqlContext.implicits._

          val df: DataFrame = List(
            // Default graph
            (
              "http://example.org/bob",
              "http://purl.org/dc/elements/1.1/publisher",
              "Bob Hacker",
              "http://example.org/dft.ttl"
            ),
            (
              "http://example.org/alice",
              "http://purl.org/dc/elements/1.1/publisher",
              "Alice Hacker",
              "http://example.org/dft.ttl"
            ),
            (
              "http://example.org/charles",
              "http://purl.org/dc/elements/1.1/publisher",
              "Charles Hacker",
              "http://example.org/dft.ttl"
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
              |PREFIX ex: <http://example.org/>
              |
              |SELECT ?x ?mbox ?name ?g
              |FROM <http://example.org/alice>
              |FROM NAMED <http://example.org/bob>
              |FROM NAMED <http://example.org/charles>
              |WHERE
              |{
              | GRAPH ?g {
              |   { ?x foaf:name ?name . }
              |   UNION
              |   { GRAPH ex:bob { ?x foaf:mbox ?mbox } }
              | }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 3
          result.right.get.collect.toSet shouldEqual Set(
            Row(
              "_:a",
              null,
              "\"Charles\"",
              "<http://example.org/charles>"
            ),
            Row(
              "_:a",
              null,
              "\"Bob\"",
              "<http://example.org/bob>"
            ),
            Row(
              "_:a",
              "<mailto:bob@oldcorp.example.org>",
              null,
              "<http://example.org/bob>"
            )
          )
        }

        // TODO: This query seems not to be consistent with Jena, shall be analyzed as a case of study
        "execute and obtain expected results when GRAPH over mixed named and default graphs with UNION" in {

          import sqlContext.implicits._

          val df: DataFrame = List(
            // Default graph
            (
              "http://example.org/bob",
              "http://purl.org/dc/elements/1.1/publisher",
              "Bob Hacker",
              "http://example.org/dft.ttl"
            ),
            (
              "http://example.org/alice",
              "http://purl.org/dc/elements/1.1/publisher",
              "Alice Hacker",
              "http://example.org/dft.ttl"
            ),
            (
              "http://example.org/charles",
              "http://purl.org/dc/elements/1.1/publisher",
              "Charles Hacker",
              "http://example.org/dft.ttl"
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
              |PREFIX ex: <http://example.org/>
              |
              |SELECT ?x ?mbox ?name
              |FROM <http://example.org/alice>
              |FROM NAMED <http://example.org/bob>
              |FROM NAMED <http://example.org/charles>
              |WHERE
              |{
              |  {
              |    GRAPH ex:bob {
              |     { ?x foaf:name ?name . }
              |     UNION
              |     { GRAPH ex:bob { ?x foaf:mbox ?mbox } }
              |    }
              |  }
              |  UNION
              |  {
              |    GRAPH ex:charles {
              |      { ?x foaf:name ?name . }
              |      UNION
              |      { GRAPH ex:bob { ?x foaf:mbox ?mbox } }
              |    }
              |  }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 4
          result.right.get.collect.toSet shouldEqual Set(
            Row(
              "_:a",
              null,
              "\"Charles\""
            ),
            Row(
              "_:a",
              "<mailto:bob@oldcorp.example.org>",
              null
            ),
            Row(
              "_:a",
              null,
              "\"Bob\""
            ),
            Row(
              "_:a",
              "<mailto:bob@oldcorp.example.org>",
              null
            )
          )
        }

        // TODO: This query seems not to be consistent with Jena, shall be analyzed as a case of study
        "execute and obtain expected results when GRAPH with variable over mixed named and default graphs with JOIN" in {

          import sqlContext.implicits._

          val df: DataFrame = List(
            // Default graph
            (
              "http://example.org/bob",
              "http://purl.org/dc/elements/1.1/publisher",
              "Bob Hacker",
              "http://example.org/dft.ttl"
            ),
            (
              "http://example.org/alice",
              "http://purl.org/dc/elements/1.1/publisher",
              "Alice Hacker",
              "http://example.org/dft.ttl"
            ),
            (
              "http://example.org/charles",
              "http://purl.org/dc/elements/1.1/publisher",
              "Charles Hacker",
              "http://example.org/dft.ttl"
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
              |PREFIX ex: <http://example.org/>
              |
              |SELECT ?x ?mbox ?name
              |FROM <http://example.org/alice>
              |FROM NAMED <http://example.org/bob>
              |FROM NAMED <http://example.org/charles>
              |WHERE
              |{
              |  GRAPH ex:bob {
              |   { ?x foaf:name ?name . }
              |   UNION
              |   { GRAPH ex:bob { ?x foaf:mbox ?mbox } }
              |  } .
              |  GRAPH ex:charles {
              |    { ?x foaf:name ?name . }
              |    UNION
              |    { GRAPH ex:bob { ?x foaf:mbox ?mbox } }
              |  }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 0
          result.right.get.collect.toSet shouldEqual Set()
        }
      }
    }

    "dealing with three column dataframes" should {
      "add the last column automatically" in {

        val df: DataFrame = List(
          (
            "example",
            "http://xmlns.com/foaf/0.1/lit",
            "\"5.88\"^^<http://www.w3.org/2001/XMLSchema#float>"
          ),
          ("example", "http://xmlns.com/foaf/0.1/lit", "\"0.22\"^^xsd:float"),
          ("example", "http://xmlns.com/foaf/0.1/lit", "\"foo\"^^xsd:string"),
          (
            "example",
            "http://xmlns.com/foaf/0.1/lit",
            "\"true\"^^xsd:boolean"
          )
        ).toDF("s", "p", "o")

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

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect().length shouldEqual 4
        result.right.get.collect().toSet shouldEqual Set(
          Row("\"5.88\"^^<http://www.w3.org/2001/XMLSchema#float>"),
          Row("\"0.22\"^^xsd:float"),
          Row("\"foo\"^^xsd:string"),
          Row("\"true\"^^xsd:boolean")
        )

      }
    }

    "dealing with wider or narrower datasets" should {
      "discard narrow ones before firing the Spark job" in {

        val df: DataFrame = List(
          "example",
          "example"
        ).toDF("s")

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

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Left[_, _]]
        result.left.get shouldEqual EngineError.InvalidInputDataFrame(
          "Input DF must have 3 or 4 columns"
        )
      }

      "discard wide ones before running the spark job" in {

        val df: DataFrame = List(
          ("example", "example", "example", "example", "example")
        ).toDF("a", "b", "c", "d", "e")

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

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Left[_, _]]
        result.left.get shouldEqual EngineError.InvalidInputDataFrame(
          "Input DF must have 3 or 4 columns"
        )
      }
    }

    "perform query with GROUP BY" should {

      "operate correctly when only GROUP BY appears" in {

        val df = List(
          (
            "http://uri.com/subject/a1",
            "http://uri.com/predicate",
            "http://uri.com/object"
          ),
          (
            "http://uri.com/subject/a1",
            "http://uri.com/predicate",
            "http://uri.com/object"
          ),
          (
            "http://uri.com/subject/a2",
            "http://uri.com/predicate",
            "http://uri.com/object"
          ),
          (
            "http://uri.com/subject/a2",
            "http://uri.com/predicate",
            "http://uri.com/object"
          ),
          (
            "http://uri.com/subject/a3",
            "http://uri.com/predicate",
            "http://uri.com/object"
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
            "http://uri.com/subject/a1",
            "http://uri.com/predicate",
            "http://uri.com/object"
          ),
          (
            "http://uri.com/subject/a1",
            "http://uri.com/predicate",
            "http://uri.com/object"
          ),
          (
            "http://uri.com/subject/a2",
            "http://uri.com/predicate",
            "http://uri.com/object"
          ),
          (
            "http://uri.com/subject/a2",
            "http://uri.com/predicate",
            "http://uri.com/object"
          ),
          (
            "http://uri.com/subject/a3",
            "http://uri.com/predicate",
            "http://uri.com/object"
          )
        ).toDF("s", "p", "o")

        val query =
          """
          SELECT ?a COUNT(?a)
          WHERE {
            ?a <http://uri.com/predicate> <http://uri.com/object>
          } GROUP BY ?a
          """

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set(
          Row("<http://uri.com/subject/a1>", "2"),
          Row("<http://uri.com/subject/a2>", "2"),
          Row("<http://uri.com/subject/a3>", "1")
        )
      }

      "operate correctly there's GROUP BY and a AVG function" in {

        val df = List(
          ("http://uri.com/subject/a1", "1", "http://uri.com/object"),
          ("http://uri.com/subject/a1", "2", "http://uri.com/object"),
          ("http://uri.com/subject/a2", "3", "http://uri.com/object"),
          ("http://uri.com/subject/a2", "4", "http://uri.com/object"),
          ("http://uri.com/subject/a3", "5", "http://uri.com/object")
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
              "http://uri.com/subject/a1",
              "http://xmlns.com/foaf/0.1/name",
              "Alice"
            ),
            (
              "http://uri.com/subject/a1",
              "http://xmlns.com/foaf/0.1/name",
              "Bob"
            ),
            (
              "http://uri.com/subject/a2",
              "http://xmlns.com/foaf/0.1/name",
              "Charles"
            ),
            (
              "http://uri.com/subject/a2",
              "http://xmlns.com/foaf/0.1/name",
              "Charlie"
            ),
            (
              "http://uri.com/subject/a3",
              "http://xmlns.com/foaf/0.1/name",
              "megan"
            ),
            (
              "http://uri.com/subject/a3",
              "http://xmlns.com/foaf/0.1/name",
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
              "http://uri.com/subject/a1",
              "http://xmlns.com/foaf/0.1/age",
              "18.1"
            ),
            (
              "http://uri.com/subject/a1",
              "http://xmlns.com/foaf/0.1/age",
              "19"
            ),
            (
              "http://uri.com/subject/a2",
              "http://xmlns.com/foaf/0.1/age",
              "30"
            ),
            (
              "http://uri.com/subject/a2",
              "http://xmlns.com/foaf/0.1/age",
              "31.5"
            ),
            (
              "http://uri.com/subject/a3",
              "http://xmlns.com/foaf/0.1/age",
              "45"
            ),
            (
              "http://uri.com/subject/a3",
              "http://xmlns.com/foaf/0.1/age",
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
              "http://uri.com/subject/a1",
              "http://xmlns.com/foaf/0.1/name",
              "Alice"
            ),
            (
              "http://uri.com/subject/a1",
              "http://xmlns.com/foaf/0.1/name",
              "Bob"
            ),
            (
              "http://uri.com/subject/a2",
              "http://xmlns.com/foaf/0.1/name",
              "Charles"
            ),
            (
              "http://uri.com/subject/a2",
              "http://xmlns.com/foaf/0.1/name",
              "Charlie"
            ),
            (
              "http://uri.com/subject/a3",
              "http://xmlns.com/foaf/0.1/name",
              "megan"
            ),
            (
              "http://uri.com/subject/a3",
              "http://xmlns.com/foaf/0.1/name",
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
              "http://uri.com/subject/a1",
              "http://xmlns.com/foaf/0.1/age",
              "18.1"
            ),
            (
              "http://uri.com/subject/a1",
              "http://xmlns.com/foaf/0.1/age",
              "19"
            ),
            (
              "http://uri.com/subject/a2",
              "http://xmlns.com/foaf/0.1/age",
              "30"
            ),
            (
              "http://uri.com/subject/a2",
              "http://xmlns.com/foaf/0.1/age",
              "31.5"
            ),
            (
              "http://uri.com/subject/a3",
              "http://xmlns.com/foaf/0.1/age",
              "45"
            ),
            (
              "http://uri.com/subject/a3",
              "http://xmlns.com/foaf/0.1/age",
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

      "operate correctly there's GROUP BY and a SUM function" in {

        val df = List(
          ("http://uri.com/subject/a1", "2", "http://uri.com/object"),
          ("http://uri.com/subject/a1", "1", "http://uri.com/object"),
          ("http://uri.com/subject/a2", "2", "http://uri.com/object"),
          ("http://uri.com/subject/a2", "1", "http://uri.com/object"),
          ("http://uri.com/subject/a3", "1", "http://uri.com/object")
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

      "operate correctly there's GROUP BY and a SAMPLE function" in {

        val df = List(
          (
            "http://uri.com/subject/a1",
            "http://uri.com/predicate/1",
            "http://uri.com/object"
          ),
          (
            "http://uri.com/subject/a1",
            "http://uri.com/predicate/2",
            "http://uri.com/object"
          ),
          (
            "http://uri.com/subject/a2",
            "http://uri.com/predicate/3",
            "http://uri.com/object"
          ),
          (
            "http://uri.com/subject/a2",
            "http://uri.com/predicate/4",
            "http://uri.com/object"
          ),
          (
            "http://uri.com/subject/a3",
            "http://uri.com/predicate/5",
            "http://uri.com/object"
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
    }

    "perform query with ORDER BY" should {

      "execute and obtain expected results when no order modifier" in {

        val df: DataFrame = List(
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Alice",
            ""
          ),
          (
            "_:c",
            "http://xmlns.com/foaf/0.1/name",
            "Charlie",
            ""
          ),
          (
            "_:b",
            "http://xmlns.com/foaf/0.1/name",
            "Bob",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?name
            |WHERE { ?x foaf:name ?name }
            |ORDER BY ?name
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect().length shouldEqual 3
        result.right.get.collect.toList shouldEqual List(
          Row("\"Alice\""),
          Row("\"Bob\""),
          Row("\"Charlie\"")
        )
      }

      "execute and obtain expected results when ASC modifier" in {

        val df: DataFrame = List(
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Alice",
            ""
          ),
          (
            "_:c",
            "http://xmlns.com/foaf/0.1/name",
            "Charlie",
            ""
          ),
          (
            "_:b",
            "http://xmlns.com/foaf/0.1/name",
            "Bob",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?name
            |WHERE { ?x foaf:name ?name }
            |ORDER BY ASC(?name)
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect().length shouldEqual 3
        result.right.get.collect.toList shouldEqual List(
          Row("\"Alice\""),
          Row("\"Bob\""),
          Row("\"Charlie\"")
        )
      }

      "execute and obtain expected results when DESC modifier" in {

        val df: DataFrame = List(
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Alice",
            ""
          ),
          (
            "_:c",
            "http://xmlns.com/foaf/0.1/name",
            "Charlie",
            ""
          ),
          (
            "_:b",
            "http://xmlns.com/foaf/0.1/name",
            "Bob",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?name
            |WHERE { ?x foaf:name ?name }
            |ORDER BY DESC(?name)
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect().length shouldEqual 3
        result.right.get.collect.toList shouldEqual List(
          Row("\"Charlie\""),
          Row("\"Bob\""),
          Row("\"Alice\"")
        )
      }

      "execute and obtain expected results whit multiple comparators" in {

        val df: DataFrame = List(
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "A. Alice",
            ""
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/age",
            "10",
            ""
          ),
          (
            "_:c",
            "http://xmlns.com/foaf/0.1/name",
            "A. Charlie",
            ""
          ),
          (
            "_:c",
            "http://xmlns.com/foaf/0.1/age",
            "30",
            ""
          ),
          (
            "_:b",
            "http://xmlns.com/foaf/0.1/name",
            "A. Bob",
            ""
          ),
          (
            "_:b",
            "http://xmlns.com/foaf/0.1/age",
            "20",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?name
            |WHERE { ?x foaf:name ?name ; foaf:age ?age }
            |ORDER BY ?name DESC(?age)
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect().length shouldEqual 3
        result.right.get.collect.toList shouldEqual List(
          Row("\"A. Alice\""),
          Row("\"A. Bob\""),
          Row("\"A. Charlie\"")
        )
      }

      "execute and obtain expected results whit multiple comparators 2" in {

        val df: DataFrame = List(
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "A. Alice",
            ""
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/age",
            "10",
            ""
          ),
          (
            "_:c",
            "http://xmlns.com/foaf/0.1/name",
            "A. Charlie",
            ""
          ),
          (
            "_:c",
            "http://xmlns.com/foaf/0.1/age",
            "30",
            ""
          ),
          (
            "_:b",
            "http://xmlns.com/foaf/0.1/name",
            "A. Bob",
            ""
          ),
          (
            "_:b",
            "http://xmlns.com/foaf/0.1/age",
            "20",
            ""
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?name
            |WHERE { ?x foaf:name ?name ; foaf:age ?age }
            |ORDER BY DESC(?name) ?age DESC(?age) ASC(?name) DESC((isBlank(?x) || isBlank(?age)))
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect().length shouldEqual 3
        result.right.get.collect.toList shouldEqual List(
          Row("\"A. Charlie\""),
          Row("\"A. Bob\""),
          Row("\"A. Alice\"")
        )
      }
    }

    "inclusive/exclusive default graph" should {

      "exclude graphs when no explicit FROM" in {

        val df: DataFrame = List(
          ("_:s1", "p1", "o1", "http://example.org/graph1"),
          ("_:s2", "p2", "o2", "http://example.org/graph2")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |SELECT ?s ?p ?o
            |WHERE { ?s ?p ?o }
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect().length shouldEqual 0
        result.right.get.collect().toSet shouldEqual Set()
      }

      "exclude graphs when explicit FROM" in {
        import sqlContext.implicits._

        val df: DataFrame = List(
          ("_:s1", "p1", "o1", "http://example.org/graph1"),
          ("_:s2", "p2", "o2", "http://example.org/graph2")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |SELECT ?s ?p ?o
            |FROM <http://example.org/graph1>
            |FROM <http://example.org/graph2>
            |WHERE { ?s ?p ?o }
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect().length shouldEqual 2
        result.right.get.collect().toSet shouldEqual Set(
          Row("_:s1", "\"p1\"", "\"o1\""),
          Row("_:s2", "\"p2\"", "\"o2\"")
        )
      }

      "include graphs when no explicit FROM" in {

        val df: DataFrame = List(
          ("_:s1", "p1", "o1", "http://example.org/graph1"),
          ("_:s2", "p2", "o2", "http://example.org/graph2")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |SELECT ?s ?p ?o
            |WHERE { ?s ?p ?o }
            |""".stripMargin

        val result = Compiler.compile(
          df,
          query,
          config.copy(isDefaultGraphExclusive = false)
        )

        result.right.get.collect().length shouldEqual 2
        result.right.get.collect().toSet shouldEqual Set(
          Row("_:s1", "\"p1\"", "\"o1\""),
          Row("_:s2", "\"p2\"", "\"o2\"")
        )
      }

      "include graphs when explicit FROM" in {

        val df: DataFrame = List(
          ("_:s1", "p1", "o1", "http://example.org/graph1"),
          ("_:s2", "p2", "o2", "http://example.org/graph2")
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |SELECT ?s ?p ?o
            |FROM <http://example.org/graph1>
            |WHERE { ?s ?p ?o }
            |""".stripMargin

        val result = Compiler.compile(
          df,
          query,
          config.copy(isDefaultGraphExclusive = false)
        )

        result.right.get.collect().length shouldEqual 2
        result.right.get.collect().toSet shouldEqual Set(
          Row("_:s1", "\"p1\"", "\"o1\""),
          Row("_:s2", "\"p2\"", "\"o2\"")
        )
      }
    }

    "perform SUBQUERY" should {

      "fail to parse" when {

        "inner CONSTRUCT as graph pattern" in {

          val df: DataFrame = List(
            (
              "http://example.org/alice",
              "http://xmlns.com/foaf/0.1/name",
              "Alice Foo",
              ""
            ),
            (
              "http://example.org/alice",
              "http://xmlns.com/foaf/0.1/name",
              "A. Foo",
              ""
            ),
            (
              "http://example.org/alice",
              "http://xmlns.com/foaf/0.1/knows",
              "http://example.org/bob",
              ""
            ),
            (
              "http://example.org/alice",
              "http://xmlns.com/foaf/0.1/knows",
              "http://example.org/carol",
              ""
            ),
            (
              "http://example.org/bob",
              "http://xmlns.com/foaf/0.1/name",
              "Bob",
              ""
            ),
            (
              "http://example.org/bob",
              "http://xmlns.com/foaf/0.1/name",
              "Bob Bar",
              ""
            ),
            (
              "http://example.org/bob",
              "http://xmlns.com/foaf/0.1/name",
              "B. Bar",
              ""
            ),
            (
              "http://example.org/carol",
              "http://xmlns.com/foaf/0.1/name",
              "Carol",
              ""
            ),
            (
              "http://example.org/carol",
              "http://xmlns.com/foaf/0.1/name",
              "Carol Baz",
              ""
            ),
            (
              "http://example.org/carol",
              "http://xmlns.com/foaf/0.1/name",
              "C. Baz",
              ""
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
              |
              |SELECT ?y ?name
              |WHERE {
              |  ?y foaf:knows ?x .
              |  {
              |    CONSTRUCT {
              |      ?x foaf:friends ?name .
              |    } WHERE {
              |      ?x foaf:name ?name . 
              |    }
              |  }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)
          result shouldBe a[Left[_, _]]
          result.left.get shouldBe a[ParsingError]
        }
      }

      "execute and obtain expected results" when {

        "outer SELECT and inner SELECT as graph pattern" in {

          val df: DataFrame = List(
            (
              "http://example.org/alice",
              "http://xmlns.com/foaf/0.1/name",
              "Alice Foo",
              ""
            ),
            (
              "http://example.org/alice",
              "http://xmlns.com/foaf/0.1/name",
              "A. Foo",
              ""
            ),
            (
              "http://example.org/alice",
              "http://xmlns.com/foaf/0.1/knows",
              "http://example.org/bob",
              ""
            ),
            (
              "http://example.org/alice",
              "http://xmlns.com/foaf/0.1/knows",
              "http://example.org/carol",
              ""
            ),
            (
              "http://example.org/bob",
              "http://xmlns.com/foaf/0.1/name",
              "Bob",
              ""
            ),
            (
              "http://example.org/bob",
              "http://xmlns.com/foaf/0.1/name",
              "Bob Bar",
              ""
            ),
            (
              "http://example.org/bob",
              "http://xmlns.com/foaf/0.1/name",
              "B. Bar",
              ""
            ),
            (
              "http://example.org/carol",
              "http://xmlns.com/foaf/0.1/name",
              "Carol",
              ""
            ),
            (
              "http://example.org/carol",
              "http://xmlns.com/foaf/0.1/name",
              "Carol Baz",
              ""
            ),
            (
              "http://example.org/carol",
              "http://xmlns.com/foaf/0.1/name",
              "C. Baz",
              ""
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
              |
              |SELECT ?y ?name
              |WHERE {
              |  ?y foaf:knows ?x .
              |  {
              |    SELECT ?x ?name
              |    WHERE {
              |      ?x foaf:name ?name .
              |    }
              |  }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 6
          result.right.get.collect.toSet shouldEqual Set(
            Row("<http://example.org/alice>", "\"Carol Baz\""),
            Row("<http://example.org/alice>", "\"Bob Bar\""),
            Row("<http://example.org/alice>", "\"Bob\""),
            Row("<http://example.org/alice>", "\"Carol\""),
            Row("<http://example.org/alice>", "\"B. Bar\""),
            Row("<http://example.org/alice>", "\"C. Baz\"")
          )
        }

        // TODO: Un-ignore when implemented ASK
        "outer SELECT and inner ASK as graph pattern" ignore {}

        "outer CONSTRUCT and inner SELECT as graph pattern" in {
          val df: DataFrame = List(
            (
              "http://example.org/alice",
              "http://xmlns.com/foaf/0.1/name",
              "Alice Foo",
              ""
            ),
            (
              "http://example.org/alice",
              "http://xmlns.com/foaf/0.1/name",
              "A. Foo",
              ""
            ),
            (
              "http://example.org/alice",
              "http://xmlns.com/foaf/0.1/knows",
              "http://example.org/bob",
              ""
            ),
            (
              "http://example.org/alice",
              "http://xmlns.com/foaf/0.1/knows",
              "http://example.org/carol",
              ""
            ),
            (
              "http://example.org/bob",
              "http://xmlns.com/foaf/0.1/name",
              "Bob",
              ""
            ),
            (
              "http://example.org/bob",
              "http://xmlns.com/foaf/0.1/name",
              "Bob Bar",
              ""
            ),
            (
              "http://example.org/bob",
              "http://xmlns.com/foaf/0.1/name",
              "B. Bar",
              ""
            ),
            (
              "http://example.org/carol",
              "http://xmlns.com/foaf/0.1/name",
              "Carol",
              ""
            ),
            (
              "http://example.org/carol",
              "http://xmlns.com/foaf/0.1/name",
              "Carol Baz",
              ""
            ),
            (
              "http://example.org/carol",
              "http://xmlns.com/foaf/0.1/name",
              "C. Baz",
              ""
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
              |
              |CONSTRUCT {
              |  ?y foaf:knows ?name .
              |} WHERE {
              |  ?y foaf:knows ?x .
              |  {
              |    SELECT ?x ?name
              |    WHERE {
              |      ?x foaf:name ?name . 
              |    }
              |  }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 6
          result.right.get.collect.toSet shouldEqual Set(
            Row(
              "<http://example.org/alice>",
              "<http://xmlns.com/foaf/0.1/knows>",
              "\"Carol Baz\""
            ),
            Row(
              "<http://example.org/alice>",
              "<http://xmlns.com/foaf/0.1/knows>",
              "\"Bob Bar\""
            ),
            Row(
              "<http://example.org/alice>",
              "<http://xmlns.com/foaf/0.1/knows>",
              "\"Bob\""
            ),
            Row(
              "<http://example.org/alice>",
              "<http://xmlns.com/foaf/0.1/knows>",
              "\"Carol\""
            ),
            Row(
              "<http://example.org/alice>",
              "<http://xmlns.com/foaf/0.1/knows>",
              "\"B. Bar\""
            ),
            Row(
              "<http://example.org/alice>",
              "<http://xmlns.com/foaf/0.1/knows>",
              "\"C. Baz\""
            )
          )
        }

        // TODO: Un-ignore when implemented ASK
        "outer CONSTRUCT and inner ASK as graph pattern" ignore {}

        // TODO: Un-ignore when implemented ASK
        "outer ASK and inner SELECT as graph pattern" ignore {}

        "multiple inner sub-queries" in {

          val df: DataFrame = List(
            (
              "http://example.org/alice",
              "http://xmlns.com/foaf/0.1/member",
              "Family",
              ""
            ),
            (
              "http://example.org/alice",
              "http://xmlns.com/foaf/0.1/name",
              "Alice Foo",
              ""
            ),
            (
              "http://example.org/alice",
              "http://xmlns.com/foaf/0.1/name",
              "A. Foo",
              ""
            ),
            (
              "http://example.org/alice",
              "http://xmlns.com/foaf/0.1/knows",
              "http://example.org/bob",
              ""
            ),
            (
              "http://example.org/alice",
              "http://xmlns.com/foaf/0.1/knows",
              "http://example.org/carol",
              ""
            ),
            (
              "http://example.org/bob",
              "http://xmlns.com/foaf/0.1/name",
              "Bob",
              ""
            ),
            (
              "http://example.org/bob",
              "http://xmlns.com/foaf/0.1/name",
              "Bob Bar",
              ""
            ),
            (
              "http://example.org/bob",
              "http://xmlns.com/foaf/0.1/name",
              "B. Bar",
              ""
            ),
            (
              "http://example.org/carol",
              "http://xmlns.com/foaf/0.1/name",
              "Carol",
              ""
            ),
            (
              "http://example.org/carol",
              "http://xmlns.com/foaf/0.1/name",
              "Carol Baz",
              ""
            ),
            (
              "http://example.org/carol",
              "http://xmlns.com/foaf/0.1/name",
              "C. Baz",
              ""
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
              |
              |SELECT ?x ?y ?name ?group
              |WHERE {
              |  ?y foaf:member ?group .
              |  {
              |    SELECT ?y ?x ?name
              |    WHERE {
              |      ?y foaf:knows ?x .
              |      {
              |         SELECT ?x ?name
              |         WHERE {
              |           ?x foaf:name ?name .
              |         }
              |      }
              |    }
              |  }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 6
          result.right.get.collect.toSet shouldEqual Set(
            Row(
              "<http://example.org/bob>",
              "<http://example.org/alice>",
              "\"B. Bar\"",
              "\"Family\""
            ),
            Row(
              "<http://example.org/carol>",
              "<http://example.org/alice>",
              "\"Carol\"",
              "\"Family\""
            ),
            Row(
              "<http://example.org/bob>",
              "<http://example.org/alice>",
              "\"Bob Bar\"",
              "\"Family\""
            ),
            Row(
              "<http://example.org/carol>",
              "<http://example.org/alice>",
              "\"C. Baz\"",
              "\"Family\""
            ),
            Row(
              "<http://example.org/bob>",
              "<http://example.org/alice>",
              "\"Bob\"",
              "\"Family\""
            ),
            Row(
              "<http://example.org/carol>",
              "<http://example.org/alice>",
              "\"Carol Baz\"",
              "\"Family\""
            )
          )
        }

        "mixing graphs" in {

          val df: DataFrame = List(
            (
              "http://example.org/alice",
              "http://xmlns.com/foaf/0.1/member",
              "Family",
              ""
            ),
            (
              "http://example.org/alice",
              "http://xmlns.com/foaf/0.1/name",
              "Alice Foo",
              "http://some-other.ttl"
            ),
            (
              "http://example.org/alice",
              "http://xmlns.com/foaf/0.1/name",
              "A. Foo",
              "http://some-other.ttl"
            ),
            (
              "http://example.org/alice",
              "http://xmlns.com/foaf/0.1/knows",
              "http://example.org/bob",
              ""
            ),
            (
              "http://example.org/alice",
              "http://xmlns.com/foaf/0.1/knows",
              "http://example.org/carol",
              ""
            ),
            (
              "http://example.org/bob",
              "http://xmlns.com/foaf/0.1/name",
              "Bob",
              "http://some-other.ttl"
            ),
            (
              "http://example.org/bob",
              "http://xmlns.com/foaf/0.1/name",
              "Bob Bar",
              "http://some-other.ttl"
            ),
            (
              "http://example.org/bob",
              "http://xmlns.com/foaf/0.1/name",
              "B. Bar",
              "http://some-other.ttl"
            ),
            (
              "http://example.org/carol",
              "http://xmlns.com/foaf/0.1/name",
              "Carol",
              "http://some-other.ttl"
            ),
            (
              "http://example.org/carol",
              "http://xmlns.com/foaf/0.1/name",
              "Carol Baz",
              "http://some-other.ttl"
            ),
            (
              "http://example.org/carol",
              "http://xmlns.com/foaf/0.1/name",
              "C. Baz",
              "http://some-other.ttl"
            )
          ).toDF("s", "p", "o", "g")

          val query =
            """
              |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
              |
              |SELECT ?x ?name 
              |FROM NAMED <http://some-other.ttl>
              |WHERE {
              |  GRAPH <http://some-other.ttl> {
              |    {
              |      SELECT ?x ?name
              |      WHERE {
              |        ?x foaf:name ?name .
              |      }
              |    }
              |  }
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result shouldBe a[Right[_, _]]
          result.right.get.collect.length shouldEqual 8
          result.right.get.collect.toSet shouldEqual Set(
            Row(
              "<http://example.org/carol>",
              "\"Carol\""
            ),
            Row(
              "<http://example.org/carol>",
              "\"Carol Baz\""
            ),
            Row(
              "<http://example.org/carol>",
              "\"C. Baz\""
            ),
            Row(
              "<http://example.org/alice>",
              "\"Alice Foo\""
            ),
            Row(
              "<http://example.org/alice>",
              "\"A. Foo\""
            ),
            Row(
              "<http://example.org/bob>",
              "\"B. Bar\""
            ),
            Row(
              "<http://example.org/bob>",
              "\"Bob\""
            ),
            Row(
              "<http://example.org/bob>",
              "\"Bob Bar\""
            )
          )
        }

        "other cases" when {

          "execute and obtain expected results from sub-query with SELECT and GROUP BY" in {

            val df: DataFrame = List(
              (
                "http://people.example/alice",
                "http://people.example/name",
                "Alice Foo",
                ""
              ),
              (
                "http://people.example/alice",
                "http://people.example/name",
                "A. Foo",
                ""
              ),
              (
                "http://people.example/alice",
                "http://people.example/knows",
                "http://people.example/bob",
                ""
              ),
              (
                "http://people.example/alice",
                "http://people.example/knows",
                "http://people.example/carol",
                ""
              ),
              (
                "http://people.example/bob",
                "http://people.example/name",
                "Bob",
                ""
              ),
              (
                "http://people.example/bob",
                "http://people.example/name",
                "Bob Bar",
                ""
              ),
              (
                "http://people.example/bob",
                "http://people.example/name",
                "B. Bar",
                ""
              ),
              (
                "http://people.example/carol",
                "http://people.example/name",
                "Carol",
                ""
              ),
              (
                "http://people.example/carol",
                "http://people.example/name",
                "Carol Baz",
                ""
              ),
              (
                "http://people.example/carol",
                "http://people.example/name",
                "C. Baz",
                ""
              )
            ).toDF("s", "p", "o", "g")

            val query =
              """
                |PREFIX peop: <http://people.example/>
                |
                |SELECT ?y ?minName
                |WHERE {
                |  peop:alice peop:knows ?y .
                |  {
                |    SELECT ?y (MIN(?name) AS ?minName)
                |    WHERE {
                |      ?y peop:name ?name .
                |    } GROUP BY ?y
                |  }
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect().length shouldEqual 2
            result.right.get.collect().toSet shouldEqual Set(
              Row("<http://people.example/bob>", "\"B. Bar\""),
              Row("<http://people.example/carol>", "\"C. Baz\"")
            )
          }
        }
      }
    }

    "perform REGEX function correctly" when {

      "used without flags" in {
        val df = List(
          (
            "http://uri.com/subject/a1",
            "http://xmlns.com/foaf/0.1/name",
            "Alice"
          ),
          (
            "http://uri.com/subject/a2",
            "http://xmlns.com/foaf/0.1/name",
            "alice"
          ),
          (
            "http://uri.com/subject/a5",
            "http://xmlns.com/foaf/0.1/name",
            "Alex"
          ),
          (
            "http://uri.com/subject/a6",
            "http://xmlns.com/foaf/0.1/name",
            "alex"
          )
        ).toDF("s", "p", "o")

        val query =
          """
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          SELECT ?name
           WHERE { ?x foaf:name  ?name
                   FILTER regex(?name, "^ali") }
          """

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set(
          Row("\"alice\"")
        )
      }

      "used with flags" in {
        val df = List(
          (
            "http://uri.com/subject/a1",
            "http://xmlns.com/foaf/0.1/name",
            "Alice"
          ),
          (
            "http://uri.com/subject/a2",
            "http://xmlns.com/foaf/0.1/name",
            "alice"
          ),
          (
            "http://uri.com/subject/a5",
            "http://xmlns.com/foaf/0.1/name",
            "Alex"
          ),
          (
            "http://uri.com/subject/a6",
            "http://xmlns.com/foaf/0.1/name",
            "alex"
          )
        ).toDF("s", "p", "o")

        val query =
          """
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          SELECT ?name
           WHERE { ?x foaf:name  ?name
                   FILTER regex(?name, "^ali", "i") }
          """

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set(
          Row("\"Alice\""),
          Row("\"alice\"")
        )
      }
    }

    "perform STRSTARTS function correctly" when {

      "used on string literals" in {
        val df = List(
          (
            "http://uri.com/subject/a1",
            "http://xmlns.com/foaf/0.1/name",
            "Alice"
          ),
          (
            "http://uri.com/subject/a2",
            "http://xmlns.com/foaf/0.1/name",
            "alice"
          ),
          (
            "http://uri.com/subject/a5",
            "http://xmlns.com/foaf/0.1/name",
            "Alex"
          ),
          (
            "http://uri.com/subject/a6",
            "http://xmlns.com/foaf/0.1/name",
            "alex"
          ),
          (
            "http://uri.com/subject/a7",
            "http://xmlns.com/foaf/0.1/name",
            "Adam"
          ),
          (
            "http://uri.com/subject/a8",
            "http://xmlns.com/foaf/0.1/name",
            "adam"
          )
        ).toDF("s", "p", "o")

        val query =
          """
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          SELECT ?name
           WHERE { ?x foaf:name  ?name
                   FILTER strstarts(?name, "al") }
          """

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set(
          Row("\"alice\""),
          Row("\"alex\"")
        )
      }
    }

    "perform STRENDS function correctly" when {

      "used on string literals" in {
        val df = List(
          (
            "http://uri.com/subject/a1",
            "http://xmlns.com/foaf/0.1/name",
            "Alice"
          ),
          (
            "http://uri.com/subject/a2",
            "http://xmlns.com/foaf/0.1/name",
            "alice"
          ),
          (
            "http://uri.com/subject/a5",
            "http://xmlns.com/foaf/0.1/name",
            "Alex"
          ),
          (
            "http://uri.com/subject/a6",
            "http://xmlns.com/foaf/0.1/name",
            "alex"
          )
        ).toDF("s", "p", "o")

        val query =
          """
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          SELECT ?name
           WHERE { ?x foaf:name  ?name
                   FILTER strends(?name, "ce") }
          """

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set(
          Row("\"Alice\""),
          Row("\"alice\"")
        )
      }
    }

    "perform STRBEFORE function correctly" when {

      "used on string literals" in {
        val df = List(
          (
            "http://uri.com/subject/#a1",
            "http://xmlns.com/foaf/0.1/name",
            "Alice#Person"
          ),
          (
            "http://uri.com/subject/#a2",
            "http://xmlns.com/foaf/0.1/name",
            "Alex#Person"
          ),
          (
            "http://uri.com/subject/#a3",
            "http://xmlns.com/foaf/0.1/name",
            "Alison"
          )
        ).toDF("s", "p", "o")

        val query =
          """
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          CONSTRUCT {
            ?x foaf:firstName ?firstName .
          }
          WHERE{
            ?x foaf:name ?name .
            BIND(STRBEFORE(?name, "#") as ?firstName) .
          }
          """

        val result = Compiler.compile(df, query, config)

        result.right.get.drop("s", "p").collect.toSet shouldEqual Set(
          Row("\"Alice\""),
          Row("\"Alex\""),
          Row("\"\"")
        )
      }
    }

    "perform STRDT function correctly" should {

      "execute and obtain expected result with URI" in {
        val df = List(
          (
            "usa",
            "http://xmlns.com/foaf/0.1/latitude",
            "123"
          ),
          (
            "usa",
            "http://xmlns.com/foaf/0.1/longitude",
            "456"
          ),
          (
            "spain",
            "http://xmlns.com/foaf/0.1/latitude",
            "789"
          ),
          (
            "spain",
            "http://xmlns.com/foaf/0.1/longitude",
            "901"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?country
            |WHERE {
            |  ?c foaf:latitude ?lat .
            |  ?c foaf:longitude ?long .
            |  BIND(strdt(?c, <http://geo.org#country>) as ?country)
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"usa\"^^<http://geo.org#country>"),
          Row("\"spain\"^^<http://geo.org#country>")
        )
      }

      "execute and obtain expected result with URI prefix" in {
        val df = List(
          (
            "usa",
            "http://xmlns.com/foaf/0.1/latitude",
            "123"
          ),
          (
            "usa",
            "http://xmlns.com/foaf/0.1/longitude",
            "456"
          ),
          (
            "spain",
            "http://xmlns.com/foaf/0.1/latitude",
            "789"
          ),
          (
            "spain",
            "http://xmlns.com/foaf/0.1/longitude",
            "901"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX geo: <http://geo.org#>
            |
            |SELECT ?country
            |WHERE {
            |  ?c foaf:latitude ?lat .
            |  ?c foaf:longitude ?long .
            |  BIND(strdt(?c, geo:country) as ?country)
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"usa\"^^<http://geo.org#country>"),
          Row("\"spain\"^^<http://geo.org#country>")
        )
      }

      // TODO: Un-ignore when CONCAT with multiple arguments
      // See: https://github.com/gsk-aiops/bellman/issues/324
      "execute and obtain expected results when complex expression" ignore {
        val df = List(
          (
            "http://example.org/usa",
            "http://xmlns.com/foaf/0.1/latitude",
            "123"
          ),
          (
            "http://example.org/usa",
            "http://xmlns.com/foaf/0.1/longitude",
            "456"
          ),
          (
            "http://example.org/spain",
            "http://xmlns.com/foaf/0.1/latitude",
            "789"
          ),
          (
            "http://example.org/spain",
            "http://xmlns.com/foaf/0.1/latitude",
            "901"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?coords
            |WHERE {
            |  ?country foaf:latitude ?lat .
            |  ?country foaf:longitude ?long .
            |  BIND(STRDT(CONCAT("country=", strafter(?country, "http://xmlns.com/foaf/0.1/"), "&long=", str(?long), "&lat=", str(?lat)), <http://geo.org/coords>) as ?coords)
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"country=usa&long=123&lat=456\"^^<http://geo.org/coords>"),
          Row("\"country=spain&long=789&lat=012\"^^<http://geo.org/coords>")
        )
      }
    }

    "perform SUBSTR function correctly" when {

      "used on string literals with a specified length" in {
        val df = List(
          (
            "http://uri.com/subject/#a1",
            "http://xmlns.com/foaf/0.1/name",
            "Alice"
          ),
          (
            "http://uri.com/subject/#a3",
            "http://xmlns.com/foaf/0.1/name",
            "Alison"
          )
        ).toDF("s", "p", "o")

        val query =
          """
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          CONSTRUCT {
            ?x foaf:subName ?subName .
          }
          WHERE{
            ?x foaf:name ?name .
            BIND(SUBSTR(?name, 1, 1) as ?subName) .
          }
          """

        val result = Compiler.compile(df, query, config)

        result.right.get.drop("s", "p").collect.toSet shouldEqual Set(
          Row("\"A\""),
          Row("\"A\"")
        )
      }

      "used on string literals without a specified length" in {
        val df = List(
          (
            "http://uri.com/subject/#a1",
            "http://xmlns.com/foaf/0.1/name",
            "Alice"
          ),
          (
            "http://uri.com/subject/#a3",
            "http://xmlns.com/foaf/0.1/name",
            "Alison"
          )
        ).toDF("s", "p", "o")

        val query =
          """
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          CONSTRUCT {
            ?x foaf:subName ?subName .
          }
          WHERE{
            ?x foaf:name ?name .
            BIND(SUBSTR(?name, 3) as ?subName) .
          }
          """

        val result = Compiler.compile(df, query, config)

        result.right.get.drop("s", "p").collect.toSet shouldEqual Set(
          Row("\"ice\""),
          Row("\"ison\"")
        )
      }
    }
  }

  private def readNTtoDF(path: String) = {

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
