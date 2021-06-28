package com.gsk.kg.engine

import com.gsk.kg.config.Config
import com.gsk.kg.engine.compiler.SparkSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.apache.jena.query.QueryFactory
import org.apache.jena.query.QueryExecutionFactory
import org.apache.jena.riot.RDFDataMgr
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._
import org.apache.jena.query.QuerySolution
import java.nio.file.Paths
import java.nio.file.Files
import org.apache.jena.riot.lang.CollectorStreamTriples
import org.apache.jena.riot.RDFParser
import org.apache.spark.sql.Column
import org.apache.jena.query.ResultSetFactory
import org.apache.spark.sql.DataFrame
import syntax._
import scala.util.Try
import scala.util.Failure
import scala.util.Success
import com.gsk.kg.sparqlparser.EngineError

class RdfTests extends AnyWordSpec with Matchers with SparkSpec {

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  val findAllSuitesQuery = """
    PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX : <http://www.w3.org/2009/sparql/docs/tests/data-sparql11/construct/manifest#>
    PREFIX rdfs:	<http://www.w3.org/2000/01/rdf-schema#>
    PREFIX mf:     <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#>
    PREFIX qt:     <http://www.w3.org/2001/sw/DataAccess/tests/test-query#>
    PREFIX dawgt:   <http://www.w3.org/2001/sw/DataAccess/tests/test-dawg#>

    SELECT *
    WHERE {
      ?suite a mf:QueryEvaluationTest .
      ?suite mf:name ?name .
      ?suite mf:result ?result .
      ?suite mf:action _:action .
      _:action qt:query ?query .
      _:action qt:data ?data .
      ?suite mf:result ?result .
    }
    """

  val suites = List(
    "aggregates/manifest.ttl",
    "bind/manifest.ttl",
    "bindings/manifest.ttl",
    "cast/manifest.ttl",
    "construct/manifest.ttl",
    "exists/manifest.ttl",
    "functions/manifest.ttl",
    "grouping/manifest.ttl",
    "negation/manifest.ttl",
    "project-expression/manifest.ttl",
    "property-path/manifest.ttl",
    "subquery/manifest.ttl",
    "syntax-query/manifest.ttl"
  )

  suites foreach { suite =>
    suite.replace("/manifest.ttl", "").capitalize should {
      val results = queryModel(findAllSuitesQuery, suite)

      results foreach { result =>
        val name = result.get("?name").toString()

        name in {
          (
            for {
              query    <- Try(readQuery(result.get("?query").toString()))
              df       <- Try(readToDF(result.get("?data").toString()))
              expected <- Try(readResults(result.get("?result").toString()))
              r <- Try(df.sparql(query)) match {
                case Failure(EngineException(err)) =>
                  Try(fail(makeCancelMessage(result, err)))
                case Failure(ex) =>
                  Try(fail(ex))
                case Success(s) =>
                  Try(s.collect shouldEqual expected.collect)
              }
            } yield ()
          ).get
        }
      }
    }
  }

  private def makeCancelMessage(
      solution: QuerySolution,
      err: EngineError
  ): String = {
    val name = solution.get("?name").toString()

    s"$name not passing, $err"
  }

  private def readQuery(pathString: String): String = {
    val cleanString = pathString.stripPrefix("file:/")
    val path        = Paths.get(cleanString)
    val file        = Files.readAllBytes(path)

    new String(file)
  }

  private def queryModel(
      queryString: String,
      modelString: String
  ): List[QuerySolution] = {
    val model     = readModel(modelString)
    val query     = QueryFactory.create(queryString)
    val execution = QueryExecutionFactory.create(query, model)
    execution.execSelect().asScala.toList
  }

  private def readModel(path: String) =
    RDFDataMgr.loadModel(
      s"modules/rdf-tests/src/test/resources/rdf-tests/sparql11/data-sparql11/$path"
    )

  private def readToDF(filename: String) = {
    import scala.collection.JavaConverters._
    import sqlContext.implicits._

    val cleanString                         = filename.stripPrefix("file:/")
    val inputStream: CollectorStreamTriples = new CollectorStreamTriples()
    RDFParser.source(cleanString).parse(inputStream)

    val df = inputStream
      .getCollected()
      .asScala
      .toList
      .map(triple =>
        (
          triple.getSubject().toString(),
          triple.getPredicate().toString(),
          triple.getObject().toString()
        )
      )
      .toDF("s", "p", "o")

    formatDF(df)
  }

  private def readResults(filename: String) = {
    import scala.collection.JavaConverters._
    import sqlContext.implicits._

    val cleanString = filename.stripPrefix("file:/")
    val resultSet   = ResultSetFactory.load(cleanString)
    val vars        = resultSet.getResultVars().asScala
    val df = resultSet.asScala.toList
      .map(solution => vars.map(v => solution.get(v).toString))
      .toDF()

    RdfFormatter.formatDataFrame(
      formatDF(df.select(explode(col(df.columns.head)))), Config.default.copy(formatRdfOutput = true))
  }

  def formatDF(df: DataFrame): DataFrame =
    df.columns.foldLeft(df) { (d, column) =>
      d.withColumn(column, format(col(column)))
    }

  def format(col: Column): Column =
    when(
      col.startsWith("http://"),
      format_string("<%s>", col)
    ).otherwise(col)

}
