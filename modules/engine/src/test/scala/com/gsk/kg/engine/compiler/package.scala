package com.gsk.kg.engine

import org.apache.jena.riot.RDFParser
import org.apache.jena.riot.lang.CollectorStreamTriples

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext

import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

package object compiler {

  def readNTtoDF(path: String)(implicit sc: SQLContext): DataFrame = {

    import scala.collection.JavaConverters._
    import sc.implicits._

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
          triple.getObject().toString()
        )
      )
      .toDF("s", "p", "o")
  }

  object Evaluation
      extends AnyWordSpec
      with Matchers
      with SparkSpec
      with TestConfig {

    val renamedColumn = "c1"

    /** This method is used by various tests to evaluate a query and check if
      * the result is equal to the expected data
      * @param df dataframe
      * @param projection function that is applied after executing the query
      * @param query
      * @param expected List with the data is expected
      * @return Assertion, success or fail
      *         p.e. To evaluate if uuid() return a valid UUID
      *         eval(
      *           List(("_:a", "<http://xmlns.com/foaf/0.1/name>", "Alice")).toDF("s", "p", "o"),
      *           Some(col(Evaluation.renamedColumn).rlike(uuidRegex)),
      *           "select uuid() where {?x foaf:name ?name}",
      *           List(Row(true))
      *         )
      */
    def eval(
        df: DataFrame,
        projection: Option[Column],
        query: String,
        expected: List[Row]
    ): Assertion = {
      val result = Compiler.compile(df, query, config)

      val dfR: DataFrame = result match {
        case Left(e)  => fail(s"test failed: $e")
        case Right(r) => r
      }

      projection
        .map(p =>
          dfR
            .withColumnRenamed(dfR.columns.head, renamedColumn)
            .select(
              p
            )
        )
        .getOrElse(dfR)
        .collect()
        .toList shouldEqual expected
    }
  }
}
