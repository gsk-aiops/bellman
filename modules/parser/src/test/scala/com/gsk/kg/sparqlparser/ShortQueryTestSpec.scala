package com.gsk.kg.sparqlparser

import cats.syntax.either._

import com.gsk.kg.sparqlparser.Expr._
import com.gsk.kg.sparqlparser.StringVal._

import org.scalatest.flatspec.AnyFlatSpec

class ShortQueryTestSpec extends AnyFlatSpec with TestUtils with TestConfig {

  "test filtered left join with multiple filters" should "pass" in {

    val q = """
    PREFIX  rdf:  <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX  dm:   <http://gsk-kg.rdip.gsk.com/dm/1.0/>
    PREFIX  litp:  <http://lit-search-api/property/>

    CONSTRUCT
    {
      ?doc litp:containsEntity ?detent .
      ?detent litp:partOfDoc ?doc .
    }
    WHERE
    {
      ?de a dm:DetectedEntity .
      OPTIONAL {
        ?de dm:predictedBy ?model .
        ?model dm:modelName ?modelname .
        BIND(STRAFTER(str(?modelname), "ner:") as ?nermodel) .
        FILTER (STRSTARTS(str(?modelname), "ner:"))
        FILTER (STRSTARTS(str(?modelname), "ner1:"))
      }
    }

    """
    parse(q, config)
      .map(_._1.r)
      .map {
        case FilteredLeftJoin(BGP(_), Extend(to, from, _), funcs) =>
          assert(funcs.size == 2)
        case _ => fail
      }
      .getOrElse(fail)
  }

  "test literal" should "create proper StringVal case classes" in {
    val q =
      """
        PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>
        PREFIX  lita:  <http://lit-search-api/attribute/>

        SELECT ?doc WHERE {
          ?s ?p true .
          "0.3"^^xsd:decimal ?p ?o .
          ?doc lita:indexEnd "-1234"^^xsd:integer .
          ?doc lita:contextText "xyz"@en .
          ?doc lita:contextText "cde" .
        }
      """
    parse(q, config)
      .map(_._1.r)
      .map {
        case Project(vs, BGP(triples)) =>
          assert(triples(0).o == BOOL("true"))
          assert(triples(1).s == NUM("0.3"))
          assert(triples(2).o == NUM("-1234"))
          assert(triples(3).o == LANG_STRING("xyz", "@en"))
          assert(triples(4).o == STRING("cde"))
        case _ => fail
      }
      .getOrElse(fail)
  }
}
