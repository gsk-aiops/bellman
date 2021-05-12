package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.engine.RdfFormatter
import com.gsk.kg.sparqlparser.EngineError
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
      "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
      "<http://id.gsk.com/dm/1.0/Document>",
      ""
    ),
    ("test", "<http://id.gsk.com/dm/1.0/docSource>", "source", "")
  )

  "Compiler" when {

    "format data type literals correctly" in {

      val df: DataFrame = List(
        (
          "example",
          "<http://xmlns.com/foaf/0.1/lit>",
          "\"5.88\"^^<http://www.w3.org/2001/XMLSchema#float>",
          ""
        ),
        (
          "example",
          "<http://xmlns.com/foaf/0.1/lit>",
          "\"0.22\"^^xsd:float",
          ""
        ),
        (
          "example",
          "<http://xmlns.com/foaf/0.1/lit>",
          "\"foo\"^^xsd:string",
          ""
        ),
        (
          "example",
          "<http://xmlns.com/foaf/0.1/lit>",
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
        ("team", "<http://xmlns.com/foaf/0.1/name>", "Anthony", ""),
        ("team", "<http://xmlns.com/foaf/0.1/name>", "Perico", ""),
        ("team", "<http://xmlns.com/foaf/0.1/name>", "Henry", "")
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

      val inputDF = readNTtoDF("fixtures/reference-q1-input.nt")(sqlContext)
      val outputDF =
        RdfFormatter.formatDataFrame(
          readNTtoDF("fixtures/reference-q1-output.nt")(sqlContext),
          config
        )

      val result = Compiler.compile(inputDF, query, config)

      result shouldBe a[Right[_, _]]
      result.right.get.collect.toSet shouldEqual outputDF
        .drop("g")
        .collect()
        .toSet
    }

    "dealing with three column dataframes" should {

      "add the last column automatically" in {

        val df: DataFrame = List(
          (
            "example",
            "<http://xmlns.com/foaf/0.1/lit>",
            "\"5.88\"^^<http://www.w3.org/2001/XMLSchema#float>"
          ),
          ("example", "<http://xmlns.com/foaf/0.1/lit>", "\"0.22\"^^xsd:float"),
          ("example", "<http://xmlns.com/foaf/0.1/lit>", "\"foo\"^^xsd:string"),
          (
            "example",
            "<http://xmlns.com/foaf/0.1/lit>",
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

    "inclusive/exclusive default graph" should {

      "exclude graphs when no explicit FROM" in {

        val df: DataFrame = List(
          ("_:s1", "p1", "o1", "<http://example.org/graph1>"),
          ("_:s2", "p2", "o2", "<http://example.org/graph2>")
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
          ("_:s1", "p1", "o1", "<http://example.org/graph1>"),
          ("_:s2", "p2", "o2", "<http://example.org/graph2>")
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
          ("_:s1", "p1", "o1", "<http://example.org/graph1>"),
          ("_:s2", "p2", "o2", "<http://example.org/graph2>")
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
          ("_:s1", "p1", "o1", "<http://example.org/graph1>"),
          ("_:s2", "p2", "o2", "<http://example.org/graph2>")
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
  }

}
