package com.gsk.kg.engine.syntax

import org.scalatest.flatspec.AnyFlatSpec
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.DataFrame

import com.gsk.kg.engine.syntax._
import org.apache.spark.sql.Row

class SyntaxSpec extends AnyFlatSpec with Matchers with DataFrameSuiteBase {

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  "df.sparql" should "run a SparQL query on a Spark DataFrame" in {
    import sqlContext.implicits._

    val df: DataFrame = List(
      (
        "test",
        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
        "<http://id.gsk.com/dm/1.0/Document>"
      ),
      ("test", "<http://id.gsk.com/dm/1.0/docSource>", "source")
    ).toDF("s", "p", "o")

    val result: DataFrame = df.sparql(
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
    )

    result.collect.toSet shouldEqual Set(
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
