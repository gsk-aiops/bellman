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
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io._
import com.gsk.kg.sparqlparser.StringVal
import scala.util.Random
import java.io.File
import scala.reflect.io.Directory

class NtriplesReaderSpec extends AnyFlatSpec with Matchers with DataFrameSuiteBase {

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  "Compiler" should "query a real DF with a real query" in {
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
    val df = Compiler.compile(inputDF, query).right.get

    noException shouldBe thrownBy {
      val path = "df_test.nt"
      // write the DF to NTriples
      df.write.ntriples(path)

      // Read the DF as NTriples
      val read = spark.read.ntriples(path)

      df.collect.toSet() shouldEqual read.collect.toSet()

      val directory = new Directory(new File(path))
      directory.deleteRecursively()
    }

    df.collect.map(format).toSet shouldEqual outputDF.collect.toSet
  }

  private def format(r: Row): Row =
    Row.fromSeq(
      r.toSeq.map {
        case str: String if str.startsWith("<") && str.endsWith(">") =>
          str.stripPrefix("<").stripSuffix(">")
        case x => s""""$x""""
      }
    )

  private def readNTtoDF(path: String) = {
    import sqlContext.implicits._

    val filename = s"modules/engine/src/test/resources/$path"

    val lang = Lang.NTRIPLES
    spark.read.rdf(lang)(filename)
  }

}
