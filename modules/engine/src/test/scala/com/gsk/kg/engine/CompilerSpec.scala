package com.gsk.kg.engine

import com.gsk.kg.sparql.syntax.all._
import com.gsk.kg.sparqlparser.Expr._
import com.gsk.kg.sparqlparser.Conditional._
import com.gsk.kg.sparqlparser.BuildInFunc._
import com.gsk.kg.sparqlparser.StringVal._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.gsk.kg.sparqlparser.QueryConstruct
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import com.gsk.kg.sparqlparser.Query
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.lang.CollectorStreamTriples
import org.apache.jena.riot.RDFParser

class CompilerSpec extends AnyFlatSpec with Matchers with DataFrameSuiteBase {

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  val dfList = List(
    (
      "test",
      "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
      "<http://id.gsk.com/dm/1.0/Document>"
    ),
    ("test", "<http://id.gsk.com/dm/1.0/docSource>", "source")
  )

  "Compiler" should "perform query operations in the dataframe" in {
    import sqlContext.implicits._

    val df = dfList.toDF("s", "p", "o")
    val query = """
      SELECT
        ?s ?p ?o
      WHERE {
        ?s ?p ?o .
      }
      """

    Compiler.compile(df, query).right.get.collect() shouldEqual Array(
    Row(
      "\"test\"",
      "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
      "<http://id.gsk.com/dm/1.0/Document>"
    ),
    Row("\"test\"", "<http://id.gsk.com/dm/1.0/docSource>", "\"source\"")
  )

  }

  it should "execute a query with two dependent BGPs" in {
    import sqlContext.implicits._

    val df: DataFrame = dfList.toDF("s", "p", "o")

    val query = """
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

  it should "execute a UNION query BGPs with the same bindings" in {
    import sqlContext.implicits._

    val df: DataFrame = (("does", "not", "match") :: dfList).toDF("s", "p", "o")

    val query = """
      SELECT
        ?s ?o
      WHERE {
        { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o }
        UNION
        { ?s <http://id.gsk.com/dm/1.0/docSource> ?o }
      }
      """

    Compiler.compile(df, query).right.get.collect() shouldEqual Array(
      Row("\"test\"", "<http://id.gsk.com/dm/1.0/Document>"),
      Row("\"test\"", "\"source\"")
    )
  }

  it should "execute a UNION query BGPs with different bindings" in {
    import sqlContext.implicits._

    val df: DataFrame = (("does", "not", "match") :: dfList).toDF("s", "p", "o")

    val query = """
      SELECT
        ?s ?o ?s2 ?o2
      WHERE {
        { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o }
        UNION
        { ?s2 <http://id.gsk.com/dm/1.0/docSource> ?o2 }
      }
      """

    Compiler.compile(df, query).right.get.collect() shouldEqual Array(
      Row("\"test\"", "<http://id.gsk.com/dm/1.0/Document>", null, null),
      Row(null, null, "\"test\"", "\"source\"")
    )
  }

  it should "execute a CONSTRUCT with a single triple pattern" in {
    import sqlContext.implicits._

    val df: DataFrame = dfList.toDF("s", "p", "o")

    val query = """
      CONSTRUCT {
        ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o
      } WHERE {
        ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o
      }
      """

    Compiler.compile(df, query).right.get.collect() shouldEqual Array(
      Row(
        "\"test\"",
        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
        "<http://id.gsk.com/dm/1.0/Document>"
      )
    )
  }

  it should "execute a CONSTRUCT with more than one triple pattern" in {
    import sqlContext.implicits._

    val positive = List(
        ("doesmatch", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "<http://id.gsk.com/dm/1.0/Document>"),
        ("doesmatchaswell", "<http://id.gsk.com/dm/1.0/docSource>", "potato")
      )
    val df: DataFrame = (positive ++ dfList).toDF("s", "p", "o")

    val query = """
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


  it should "execute a CONSTRUCT with more than one triple pattern with common bindings" in {
    import sqlContext.implicits._

    val negative = List(
        ("doesntmatch", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "http://id.gsk.com/dm/1.0/Document>"),
        ("doesntmatcheither", "<http://id.gsk.com/dm/1.0/docSource>", "potato")
      )

    val df: DataFrame = (negative ++ dfList).toDF("s", "p", "o")

    val query = """
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


  /**
    * TODO(pepegar): In order to make this test pass we need the
    * results to be RDF compliant (mainly, wrapping values correctly)
    */
  it should "query a real DF with a real query" ignore {
    val query = """
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

    val outputDF = readNTtoDF("fixtures/reference-q1-output.nt")

    Compiler.compile(inputDF, query) shouldBe a[Right[_, _]]
    Compiler.compile(inputDF, query).right.get.collect.toSet shouldEqual outputDF.collect().toSet
  }

  it should "query a real DF with limit greater than 0 and obtain a correct result" in {
    import sqlContext.implicits._

    val df: DataFrame = List(
      ("a", "b", "c"),
      ("team", "<http://xmlns.com/foaf/0.1/name>", "Anthony"),
      ("team", "<http://xmlns.com/foaf/0.1/name>", "Perico"),
      ("team", "<http://xmlns.com/foaf/0.1/name>", "Henry")
    ).toDF("s", "p", "o")

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
    result.right.get.collect.toSet shouldEqual Set(Row("\"Anthony\""), Row("\"Perico\""))
  }

  it should "query a real DF with limit equal to 0 and obtain no results" in {
    import sqlContext.implicits._

    val df: DataFrame = List(
      ("\"a\"", "\"b\"", "\"c\""),
      ("\"team\"", "<http://xmlns.com/foaf/0.1/name>", "\"Anthony\""),
      ("\"team\"", "<http://xmlns.com/foaf/0.1/name>", "\"Perico\""),
      ("\"team\"", "<http://xmlns.com/foaf/0.1/name>", "\"Henry\"")
    ).toDF("s", "p", "o")

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

  it should "query a real DF with limit greater than Java MAX INTEGER and obtain an error" in {
    import sqlContext.implicits._

    val df: DataFrame = List(
      ("a", "b", "c"),
      ("team", "<http://xmlns.com/foaf/0.1/name>", "Anthony"),
      ("team", "<http://xmlns.com/foaf/0.1/name>", "Perico"),
      ("team", "<http://xmlns.com/foaf/0.1/name>", "Henry")
    ).toDF("s", "p", "o")

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
    result.left.get shouldEqual EngineError.NumericTypesDoNotMatch("2147483648 to big to be converted to an Int")
  }

  it should "query a real DF with offset greater than 0 and obtain a non empty set" in {
    import sqlContext.implicits._

    val df: DataFrame = List(
      ("a", "b", "c"),
      ("team", "<http://xmlns.com/foaf/0.1/name>", "Anthony"),
      ("team", "<http://xmlns.com/foaf/0.1/name>", "Perico"),
      ("team", "<http://xmlns.com/foaf/0.1/name>", "Henry")
    ).toDF("s", "p", "o")

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
    result.right.get.collect.toSet shouldEqual Set(Row("\"Perico\""), Row("\"Henry\""))
  }

  it should "query a real DF with offset equal to 0 and obtain same elements as the original set" in {
    import sqlContext.implicits._

    val df: DataFrame = List(
      ("a", "b", "c"),
      ("team", "<http://xmlns.com/foaf/0.1/name>", "Anthony"),
      ("team", "<http://xmlns.com/foaf/0.1/name>", "Perico"),
      ("team", "<http://xmlns.com/foaf/0.1/name>", "Henry")
    ).toDF("s", "p", "o")

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
    result.right.get.collect.toSet shouldEqual Set(Row("\"Anthony\""), Row("\"Perico\""), Row("\"Henry\""))
  }

  it should "query a real DF with offset greater than the number of elements of the dataframe and obtain an empty set" in {
    import sqlContext.implicits._

    val df: DataFrame = List(
      ("a", "b", "c"),
      ("team", "<http://xmlns.com/foaf/0.1/name>", "Anthony"),
      ("team", "<http://xmlns.com/foaf/0.1/name>", "Perico"),
      ("team", "<http://xmlns.com/foaf/0.1/name>", "Henry")
    ).toDF("s", "p", "o")

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

  private def readNTtoDF(path: String) = {
    import sqlContext.implicits._
    import scala.collection.JavaConverters._

    val filename = s"modules/engine/src/test/resources/$path"
    val inputStream: CollectorStreamTriples = new CollectorStreamTriples();
    RDFParser.source(filename).parse(inputStream);

    inputStream
      .getCollected()
      .asScala
      .toList
      .map(triple =>
        (triple.getSubject().toString(), triple.getPredicate().toString(), triple.getObject().toString())
      ).toDF("s", "p", "o")

  }

}
