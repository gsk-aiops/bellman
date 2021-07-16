package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import com.gsk.kg.engine.Compiler
import com.gsk.kg.engine.RdfFormatter
import com.gsk.kg.engine.functions.Literals
import com.gsk.kg.sparqlparser.TestConfig
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.substring_index
import org.apache.spark.sql.functions.trim
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.types.StringType
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SameTermSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "perform SAMETERM query" should {

    "execute and obtain expected results" when {

      "simple query" in {

        val df: DataFrame = List(
          ("_:a", "<http://xmlns.com/foaf/0.1/name>", "Alice"),
          (
            "_:a",
            "<http://xmlns.com/foaf/0.1/mbox>",
            "<mailto:alice@work.example>"
          ),
          ("_:b", "<http://xmlns.com/foaf/0.1/name>", "Ms A."),
          (
            "_:b",
            "<http://xmlns.com/foaf/0.1/mbox>",
            "<mailto:alice@work.example>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?name1 ?name2
            |WHERE { 
            | ?x foaf:name  ?name1 ;
            | foaf:mbox  ?mbox1 .
            | ?y foaf:name  ?name2 ;
            | foaf:mbox  ?mbox2 .
            | FILTER (sameTerm(?mbox1, ?mbox2) && !sameTerm(?name1, ?name2))
            |} 
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"Alice\"", "\"Ms A.\""),
          Row("\"Ms A.\"", "\"Alice\"")
        )
      }

      "simple query with non-equivalent typed literals" in {

        val df: DataFrame = List(
          (
            "_:c1",
            "<http://example.org/WMterms#label>",
            "Container 1"
          ),
          (
            "_:c1",
            "<http://example.org/WMterms#weight>",
            "\"100\"^^<http://example.org/types#kilos>"
          ),
          (
            "_:c1",
            "<http://example.org/WMterms#displacement>",
            "\"100\"^^<http://example.org/types#liters>"
          ),
          (
            "_:c2",
            "<http://example.org/WMterms#label>",
            "Container 2"
          ),
          (
            "_:c2",
            "<http://example.org/WMterms#weight>",
            "\"100\"^^<http://example.org/types#kilos>"
          ),
          (
            "_:c2",
            "<http://example.org/WMterms#displacement>",
            "\"85\"^^<http://example.org/types#liters>"
          ),
          (
            "_:c3",
            "<http://example.org/WMterms#label>",
            "Container 3"
          ),
          (
            "_:c3",
            "<http://example.org/WMterms#weight>",
            "\"85\"^^<http://example.org/types#kilos>"
          ),
          (
            "_:c3",
            "<http://example.org/WMterms#displacement>",
            "\"85\"^^<http://example.org/types#liters>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX  :      <http://example.org/WMterms#>
            |PREFIX  t:     <http://example.org/types#>
            |
            |SELECT ?aLabel ?bLabel
            |WHERE { ?a  :label        ?aLabel .
            |        ?a  :weight       ?aWeight .
            |        ?a  :displacement ?aDisp .
            |
            |        ?b  :label        ?bLabel .
            |        ?b  :weight       ?bWeight .
            |        ?b  :displacement ?bDisp .
            |        FILTER ( sameTerm(?aWeight, ?bWeight) && !sameTerm(?aDisp, ?bDisp)) }
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"Container 1\"", "\"Container 2\""),
          Row("\"Container 2\"", "\"Container 1\"")
        )
      }

      "simple query 2" in {

        val df: DataFrame = List(
          (
            "<http://example.org/ns#a>",
            "<http://example.org/ns#p>",
            "\"3\"^^xsd:integer"
          ),
          (
            "<http://example.org/ns#b>",
            "<http://example.org/ns#p>",
            "\"3\""
          ),
          (
            "<http://example.org/ns#b>",
            "<http://example.org/ns#p>",
            "3"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX ns:   <http://example.org/ns#>
            |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
            |
            |SELECT ?s
            |WHERE { ?x ns:p ?y .
            |        bind(sameTerm("3"^^xsd:string, ?y) as ?s)
            |        }
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.show(false)
      }
    }
  }
}
