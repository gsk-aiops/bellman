package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class IsBlankSpec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
    with TestConfig {

  import sqlContext.implicits._

  "perform query with ISBLANK function" should {

    "execute and obtain expected results" in {

      val df: DataFrame = List(
        (
          "_:a",
          "<http://www.w3.org/2000/10/annotation-ns#annotates>",
          "<http://www.w3.org/TR/rdf-sparql-query/>",
          ""
        ),
        (
          "_:a",
          "<http://purl.org/dc/elements/1.1/creator>",
          "Alice B. Toeclips",
          ""
        ),
        (
          "_:b",
          "<http://www.w3.org/2000/10/annotation-ns#annotates>",
          "<http://www.w3.org/TR/rdf-sparql-query/>",
          ""
        ),
        ("_:b", "<http://purl.org/dc/elements/1.1/creator>", "_:c", ""),
        ("_:c", "<http://xmlns.com/foaf/0.1/given>", "Bob", ""),
        ("_:c", "<http://xmlns.com/foaf/0.1/family>", "Smith", "")
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
}
