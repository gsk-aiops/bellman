package com.gsk.kg.engine

import com.gsk.kg.sparql.syntax.all._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.DataFrame
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.jena.riot.lang.CollectorStreamTriples
import org.apache.jena.riot.RDFParser

class CompilerSpec extends AnyFlatSpec with Matchers with DataFrameSuiteBase {

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  val dfList = List(
    (
      "test",
      "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
      "http://id.gsk.com/dm/1.0/Document"
    ),
    ("test", "http://id.gsk.com/dm/1.0/docSource", "source")
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
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
        "http://id.gsk.com/dm/1.0/Document"
      ),
      Row("\"test\"", "http://id.gsk.com/dm/1.0/docSource", "\"source\"")
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
      Row("\"test\"", "http://id.gsk.com/dm/1.0/Document"),
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
      Row("\"test\"", "http://id.gsk.com/dm/1.0/Document", null, null),
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
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
        "http://id.gsk.com/dm/1.0/Document"
      )
    )
  }

  it should "execute a CONSTRUCT with more than one triple pattern" in {
    import sqlContext.implicits._

    val positive = List(
        ("doesmatch", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://id.gsk.com/dm/1.0/Document"),
        ("doesmatchaswell", "http://id.gsk.com/dm/1.0/docSource", "potato")
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


  it should "execute a CONSTRUCT with more than one triple pattern with common bindings" in {
    import sqlContext.implicits._

    val negative = List(
        ("doesntmatch", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://id.gsk.com/dm/1.0/Document"),
        ("doesntmatcheither", "http://id.gsk.com/dm/1.0/docSource", "potato")
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


  /**
    * TODO(pepegar): In order to make this test pass we need the
    * results to be RDF compliant (mainly, wrapping values correctly)
    */
  it should "query a real DF with a real query" in {
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
      ("team", "http://xmlns.com/foaf/0.1/name", "Anthony"),
      ("team", "http://xmlns.com/foaf/0.1/name", "Perico"),
      ("team", "http://xmlns.com/foaf/0.1/name", "Henry")
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
      ("\"team\"", "http://xmlns.com/foaf/0.1/name", "\"Anthony\""),
      ("\"team\"", "http://xmlns.com/foaf/0.1/name", "\"Perico\""),
      ("\"team\"", "http://xmlns.com/foaf/0.1/name", "\"Henry\"")
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
      ("team", "http://xmlns.com/foaf/0.1/name", "Anthony"),
      ("team", "http://xmlns.com/foaf/0.1/name", "Perico"),
      ("team", "http://xmlns.com/foaf/0.1/name", "Henry")
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
      ("team", "http://xmlns.com/foaf/0.1/name", "Anthony"),
      ("team", "http://xmlns.com/foaf/0.1/name", "Perico"),
      ("team", "http://xmlns.com/foaf/0.1/name", "Henry")
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
      ("team", "http://xmlns.com/foaf/0.1/name", "Anthony"),
      ("team", "http://xmlns.com/foaf/0.1/name", "Perico"),
      ("team", "http://xmlns.com/foaf/0.1/name", "Henry")
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
      ("team", "http://xmlns.com/foaf/0.1/name", "Anthony"),
      ("team", "http://xmlns.com/foaf/0.1/name", "Perico"),
      ("team", "http://xmlns.com/foaf/0.1/name", "Henry")
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

  it should "support blank nodes in queries" in {
    import sqlContext.implicits._

    val df: DataFrame = List(
       ("nodeA", "http://gsk-kg.rdip.gsk.com/dm/1.0/predEntityClass", "thisIsTheBlankNode"),
       ("thisIsTheBlankNode", "http://gsk-kg.rdip.gsk.com/dm/1.0/predClass", "otherThingy")
    ).toDF("s", "p", "o")

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

  it should "query a real DF with REPLACE function and obtain expected results" in {
    import sqlContext.implicits._

    val df: DataFrame = List(
      ("example", "http://xmlns.com/foaf/0.1/lit", "abcd"),
      ("example", "http://xmlns.com/foaf/0.1/lit", "abaB"),
      ("example", "http://xmlns.com/foaf/0.1/lit", "bbBB"),
      ("example", "http://xmlns.com/foaf/0.1/lit", "aaaa")
    ).toDF("s", "p", "o")

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
  it should "query a real DF with REPLACE function and obtain an expected error, " +
    "because the pattern matches the zero-length string" ignore {
    import sqlContext.implicits._

    val df: DataFrame = List(
      ("example", "http://xmlns.com/foaf/0.1/lit", "abracadabra")
    ).toDF("s", "p", "o")

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
    result.left.get shouldEqual EngineError.FunctionError(s"Error on REPLACE function: No group 1")
  }

  it should "query a real DF with ISBLANK function and obtain expected results" in {
    import sqlContext.implicits._

    val df: DataFrame = List(
      ("_:a", "http://www.w3.org/2000/10/annotation-ns#annotates", "http://www.w3.org/TR/rdf-sparql-query/"),
      ("_:a", "http://purl.org/dc/elements/1.1/creator", "Alice B. Toeclips"),
      ("_:b", "http://www.w3.org/2000/10/annotation-ns#annotates", "http://www.w3.org/TR/rdf-sparql-query/"),
      ("_:b", "http://purl.org/dc/elements/1.1/creator", "_:c"),
      ("_:c", "http://xmlns.com/foaf/0.1/given", "Bob"),
      ("_:c", "http://xmlns.com/foaf/0.1/family", "Smith")
    ).toDF("s", "p", "o")

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

  it should "format data type literals correctly" in {
    import sqlContext.implicits._

    val df: DataFrame = List(
      ("example", "http://xmlns.com/foaf/0.1/lit", "\"5.88\"^^<http://www.w3.org/2001/XMLSchema#float>"),
      ("example", "http://xmlns.com/foaf/0.1/lit", "\"0.22\"^^xsd:float"),
      ("example", "http://xmlns.com/foaf/0.1/lit", "\"foo\"^^xsd:string"),
      ("example", "http://xmlns.com/foaf/0.1/lit", "\"true\"^^xsd:boolean")
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

  it should "query a real DF with FILTER and obtain expected results when only one condition" in {

    import sqlContext.implicits._

    val df: DataFrame = List(
      ("a", "b", "c"),
      ("team", "http://xmlns.com/foaf/0.1/name", "Anthony"),
      ("team", "http://xmlns.com/foaf/0.1/name", "Perico"),
      ("team", "http://xmlns.com/foaf/0.1/name", "Henry"),
      ("_:", "http://xmlns.com/foaf/0.1/name", "Blank")
    ).toDF("s", "p", "o")

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

  // TODO: Un-ignore when binary logical operations implemented
  it should "query a real DF with FILTER and obtain expected results when multiple conditions" ignore {

    import sqlContext.implicits._

    val df: DataFrame = List(
      ("a", "b", "c"),
      ("team", "http://xmlns.com/foaf/0.1/name", "Anthony"),
      ("team", "http://xmlns.com/foaf/0.1/name", "Perico"),
      ("team", "http://xmlns.com/foaf/0.1/name", "Henry"),
      ("_:", "http://xmlns.com/foaf/0.1/name", "Blank"),
      ("_:", "http://xmlns.com/foaf/0.1/name", "http://test-uri/blank")
    ).toDF("s", "p", "o")

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

  it should "query a real DF with FILTER and obtain expected results when condition has embedded functions" in {

    import sqlContext.implicits._

    val df: DataFrame = List(
      ("a", "b", "c"),
      ("team", "http://xmlns.com/foaf/0.1/name", "Anthony"),
      ("team", "http://xmlns.com/foaf/0.1/name", "Perico"),
      ("team", "http://xmlns.com/foaf/0.1/name", "Henry"),
      ("a:", "http://xmlns.com/foaf/0.1/name", "Blank")
    ).toDF("s", "p", "o")

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

  it should "query a real DF with FILTER and obtain expected results when double filter" in {

    import sqlContext.implicits._

    val df: DataFrame = List(
      ("a", "b", "c"),
      ("team", "http://xmlns.com/foaf/0.1/name", "Anthony"),
      ("_:a", "http://xmlns.com/foaf/0.1/name", "_:b"),
      ("foaf:c", "http://xmlns.com/foaf/0.1/name", "_:d"),
      ("_:e", "http://xmlns.com/foaf/0.1/name", "foaf:f")
    ).toDF("s", "p", "o")

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

  it should "query a real DF with FILTER and obtain expected results when filter over all select statement" in {

    import sqlContext.implicits._

    val df: DataFrame = List(
      ("http://example.org/Network", "http://www.w3.org/2000/01/rdf-schema#subClassOf", "http://example.org/Main"),
      ("http://example.org/ATM", "http://www.w3.org/2000/01/rdf-schema#subClassOf", "http://example.org/Network"),
      ("http://example.org/ARPANET", "http://www.w3.org/2000/01/rdf-schema#subClassOf", "http://example.org/Network"),
      ("http://example.org/Software", "http://www.w3.org/2000/01/rdf-schema#subClassOf", "http://example.org/Main"),
      ("_:Linux", "http://www.w3.org/2000/01/rdf-schema#subClassOf", "http://example.org/Software"),
      ("http://example.org/Windows", "http://www.w3.org/2000/01/rdf-schema#subClassOf", "http://example.org/Software"),
      ("http://example.org/XP", "http://www.w3.org/2000/01/rdf-schema#subClassOf", "http://example.org/Windows"),
      ("http://example.org/Win7", "http://www.w3.org/2000/01/rdf-schema#subClassOf", "http://example.org/Windows"),
      ("http://example.org/Win8", "http://www.w3.org/2000/01/rdf-schema#subClassOf", "http://example.org/Windows"),
      ("http://example.org/Ubuntu20", "http://www.w3.org/2000/01/rdf-schema#subClassOf", "_:Linux")
    ).toDF("s", "p", "o")

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
  it should "query a real DF with FILTER and obtain expected results when complex filter" ignore {

    import sqlContext.implicits._

    val df: DataFrame = List(
      ("_:a", "http://xmlns.com/foaf/0.1/name", "Alice"),
      ("_:a", "http://example.org/stats#hits", "\"2349\"^^xsd:integer"),
      ("_:b", "http://xmlns.com/foaf/0.1/name", "Bob"),
      ("_:b", "http://example.org/stats#hits", "\"105\"^^xsd:integer"),
      ("_:c", "http://xmlns.com/foaf/0.1/name", "Eve"),
      ("_:c", "http://example.org/stats#hits", "\"181\"^^xsd:integer")
    ).toDF("s", "p", "o")

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
      Row("_:a", "foaf:name", "\"Bob\""),
      Row("_:b", "site:hits", "\"2349\"^^xsd:integer")
    )
  }

  it should "apply default ordering CONSTRUCT queries" in {
    import sqlContext.implicits._

    val df: DataFrame = List(
        ("http://potato.com/b", "http://gsk-kg.rdip.gsk.com/dm/1.0/docSource", "http://thesour.ce"),
        ("http://potato.com/c", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://gsk-kg.rdip.gsk.com/dm/1.0/Document"),
        ("http://potato.com/c", "http://gsk-kg.rdip.gsk.com/dm/1.0/docSource", "http://thesour.ce"),
        ("http://potato.com/d", "http://gsk-kg.rdip.gsk.com/dm/1.0/docSource", "http://thesour.ce"),
        ("http://potato.com/b", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://gsk-kg.rdip.gsk.com/dm/1.0/Document"),
        ("http://potato.com/d", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://gsk-kg.rdip.gsk.com/dm/1.0/Document"),
        ("negative", "negative", "negative")
      ).toDF("s", "p", "o")


    val query = """
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
        Row("http://potato.com/b", "http://gsk-kg.rdip.gsk.com/dm/1.0/docSource", "http://thesour.ce"),
        Row("http://potato.com/b", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://gsk-kg.rdip.gsk.com/dm/1.0/Document"),
        Row("http://potato.com/c", "http://gsk-kg.rdip.gsk.com/dm/1.0/docSource", "http://thesour.ce"),
        Row("http://potato.com/c", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://gsk-kg.rdip.gsk.com/dm/1.0/Document"),
        Row("http://potato.com/d", "http://gsk-kg.rdip.gsk.com/dm/1.0/docSource", "http://thesour.ce"),
        Row("http://potato.com/d", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://gsk-kg.rdip.gsk.com/dm/1.0/Document")
      )
  }

  it should "make sense in the LIMIT cause when there's no ORDER BY" in {
    import sqlContext.implicits._

    val df: DataFrame = List(
        ("http://potato.com/b", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://gsk-kg.rdip.gsk.com/dm/1.0/Document"),
        ("http://potato.com/c", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://gsk-kg.rdip.gsk.com/dm/1.0/Document"),
        ("http://potato.com/b", "http://gsk-kg.rdip.gsk.com/dm/1.0/docSource", "http://thesour.ce"),
        ("http://potato.com/d", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://gsk-kg.rdip.gsk.com/dm/1.0/Document"),
        ("http://potato.com/c", "http://gsk-kg.rdip.gsk.com/dm/1.0/docSource", "http://thesour.ce"),
        ("http://potato.com/d", "http://gsk-kg.rdip.gsk.com/dm/1.0/docSource", "http://thesour.ce"),
        ("negative", "negative", "negative")
      ).toDF("s", "p", "o")


    val query = """
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
      Row("http://potato.com/b", "http://gsk-kg.rdip.gsk.com/dm/1.0/docSource", "http://thesour.ce"),
      Row("http://potato.com/b", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://gsk-kg.rdip.gsk.com/dm/1.0/Document")
    )
  }

  it should "query a real DF with OPTIONAL and obtain expected results with simple optional" in {

    import sqlContext.implicits._

    val df: DataFrame = List(
      ("_:a", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://xmlns.com/foaf/0.1/Person"),
      ("_:a", "http://xmlns.com/foaf/0.1/name", "Alice"),
      ("_:a", "http://xmlns.com/foaf/0.1/mbox", "mailto:alice@example.com"),
      ("_:a", "http://xmlns.com/foaf/0.1/mbox", "mailto:alice@work.example"),
      ("_:b", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://xmlns.com/foaf/0.1/Person"),
      ("_:b", "http://xmlns.com/foaf/0.1/name", "Bob")
    ).toDF("s", "p", "o")

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

  it should "query a real DF with OPTIONAL and obtain expected results with constraints in optional" in {
    import sqlContext.implicits._

    val df: DataFrame = List(
      ("_:book1", "http://purl.org/dc/elements/1.1/title", "SPARQL Tutorial"),
      ("_:book1", "http://example.org/ns#price", "42"),
      ("_:book2", "http://purl.org/dc/elements/1.1/title", "The Semantic Web"),
      ("_:book2", "http://example.org/ns#price", "_:23")
    ).toDF("s", "p", "o")

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

  it should "query a real DF with OPTIONAL and obtain expected results with multiple optionals" in {
    import sqlContext.implicits._

    val df: DataFrame = List(
      ("_:a", "http://xmlns.com/foaf/0.1/name", "Alice"),
      ("_:a", "http://xmlns.com/foaf/0.1/homepage", "http://work.example.org/alice/"),
      ("_:b", "http://xmlns.com/foaf/0.1/name", "Bob"),
      ("_:b", "http://xmlns.com/foaf/0.1/mbox", "mailto:bob@work.example")
    ).toDF("s", "p", "o")

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

  it should "query a real DF with negated (!) isBlank" in {
    import sqlContext.implicits._

    val df: DataFrame = List(
      ("_:a", "http://xmlns.com/foaf/0.1/name", "Alice"),
      ("_:a", "http://xmlns.com/foaf/0.1/mbox", "mailto:alice@work.example"),
      ("_:b", "http://xmlns.com/foaf/0.1/name", "_:bob"),
      ("_:b", "http://xmlns.com/foaf/0.1/mbox", "mailto:bob@work.example")
    ).toDF("s", "p", "o")


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
