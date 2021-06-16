package com.gsk.kg.engine.compiler

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class UUIDSpec
  extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  /*
  https://www.w3.org/TR/sparql11-query/#func-uuid
  UUID() -> <urn:uuid:b9302fb5-642e-4d3b-af19-29a8f6d894c9>
   */

  "perform uuid function correctly" when {
    "uuid response with an UUID valid" in {
      val str = "abc"
      val expected = Row("")
      val actual = act(str)
      val df = actual match {
        case Left(e) => throw new Exception(e.toString)
        case Right(r) => r
      }
      df.show(false)
    }
  }

  private def act(str: String) = {

    val df: DataFrame = List(
      ("_:a", "<http://xmlns.com/foaf/0.1/name>", "Alice", ""),
      ("_:b", "<http://xmlns.com/foaf/0.1/name>", "Bob", ""),
      ("_:c", "<http://xmlns.com/foaf/0.1/name>", "Alice", "")
    ).toDF("s", "p", "o", "g")


    val query =
    """
      |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
      |
      |SELECT UUID()
      |WHERE  {
      |   ?x foaf:name ?name
      |}
      |""".stripMargin

      Compiler
      .compile(df, query, config)
//      .right
//      .get
//      .drop("s", "p")
//      .head()
  }

}
