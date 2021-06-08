package com.gsk.kg.engine
package compiler

import com.gsk.kg.engine.syntax._

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class Bug3255Spec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase {

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  "AIPL-3255 bug" should {

    "allow queries with two triples with a single variable in the same position" in {

      import sqlContext.implicits._

      val query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX xml: <http://www.w3.org/XML/1998/namespace>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX prism: <http://prismstandard.org/namespaces/basic/2.0/>
        PREFIX schema: <http://schema.org/>
        PREFIX mrsat: <http://gsk-kg.rdip.gsk.com/umls/MRSAT#>

        SELECT ?s
        WHERE {
          ?s mrsat:ATN "SWP" .
          ?s mrsat:SUPRESS "N" .
        }
        """

        val df = List(
          ("asdf", "<http://gsk-kg.rdip.gsk.com/umls/MRSAT#ATN>", "SWP"),
          ("asdf", "<http://gsk-kg.rdip.gsk.com/umls/MRSAT#SUPRESS>", "N")
        ).toDF("s", "p", "o")

        val response = df.sparql(query)

        response.show
    }

  }

}

