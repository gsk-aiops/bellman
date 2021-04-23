package com.gsk.kg.sparqlparser

import com.gsk.kg.sparql.syntax.Interpolators._

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class InterpolatorsSpec extends AnyWordSpec with Matchers {

  "Sparql interpolator" should {

    "interpolate with default config" in {

      val query = sparql"""
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        SELECT ?s ?o
        WHERE { ?s foaf:name ?o }
      """

      query shouldBe a[Query]
    }
  }
}
