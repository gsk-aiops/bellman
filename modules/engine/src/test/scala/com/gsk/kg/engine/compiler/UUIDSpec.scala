package com.gsk.kg.engine.compiler

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig
import org.apache.spark.sql.Row
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
      actual.show(false)
//      actual.isRight shouldEqual(true)
//      actual.show(false)
    }
  }

  private def act(str: String) = {
    val df = List(
      (
        "<http://uri.com/subject/#a1>",
        "<http://xmlns.com/foaf/0.1/title>",
        str
      )
    ).toDF("s", "p", "o")

    val query =
      s"""
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          CONSTRUCT {
            ?x foaf:titleUpper ?titleUpper .
          }
          WHERE{
            ?x foaf:title ?title .
            BIND(UCASE(?title) as ?titleUpper) .
          }
          """
      Compiler
      .compile(df, query, config)
      .right
      .get
      .drop("s", "p")
//      .head()
  }

}
