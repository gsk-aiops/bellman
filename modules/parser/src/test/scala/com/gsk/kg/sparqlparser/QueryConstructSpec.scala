package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.Expr._
import com.gsk.kg.sparqlparser.Query._
import com.gsk.kg.sparqlparser.StringVal._
import org.scalatest.flatspec.AnyFlatSpec
import scala.collection.mutable

class QueryConstructSpec extends AnyFlatSpec {

  "Simple Query" should "parse Construct statement with correct number of Triples" in {
    TestUtils.query("/queries/q0-simple-basic-graph-pattern.sparql") match {
      case Construct(vars, bgp, expr, List(), List()) =>
        assert(vars.size == 2 && bgp.triples.size == 2)
      case _ => fail
    }
  }


  "Construct" should "result in proper variables, a basic graph pattern, and algebra expression" in {
    TestUtils.query("/queries/q3-union.sparql") match {
      case Construct(vars, bgp, Union(BGP(triplesL: Seq[Triple]), BGP(triplesR: Seq[Triple])), List(), List()) =>
        val temp = QueryConstruct.getAllVariableNames(bgp)
        val all = vars.map(_.s).toSet
        assert((all -- temp) == Set("?lnk"))
      case _ => fail
    }
  }


  "Construct with Bind" should "contains bind variable" in {
    TestUtils.query("/queries/q4-simple-bind.sparql") match {
      case Construct(vars, bgp, Extend(l: StringVal, r: StringVal, BGP(triples: Seq[Triple])), List(), List()) =>
        vars.exists(_.s == "?dbind")
      case _ => fail
    }
  }

  "Complex named graph query" should "be captured properly in Construct" in {
    TestUtils.query("/queries/q13-complex-named-graph.sparql") match {
      case Construct(vars, bgp, expr, List(), List()) =>
        assert(vars.size == 13)
        assert(vars.exists(va => va.s == "?ogihw"))
      case _ => fail
    }
  }

  "Complex lit-search query" should "return proper Construct type" in {
    val a = 1
    TestUtils.query("/queries/lit-search-3.sparql") match {
      case Construct(vars, bgp, expr, List(), List()) =>
        assert(bgp.triples.size == 11)
        assert(bgp.triples.head.o.asInstanceOf[BLANK].s == bgp.triples(1).s.asInstanceOf[BLANK].s)
        assert(vars.exists(v => v.s == "?secid"))
      case _ => fail
    }
  }

  "Extra large query" should "return proper Construct type" in {
    TestUtils.query("/queries/lit-search-xlarge.sparql") match {
      case Construct(vars, bgp, expr, List(), List()) =>
        assert(bgp.triples.size == 67)
        assert(bgp.triples.head.s.asInstanceOf[VARIABLE].s == "?Year")
        assert(bgp.triples.last.s.asInstanceOf[VARIABLE].s == "?Predication")
        assert(vars.exists(v => v.s == "?de"))
      case _ => fail
    }
  }

  "Select query" should "be supported even it is nested" in {
    val query =
      """
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX  dm:  <http://gsk-kg.rdip.gsk.com/dm/1.0/>
        CONSTRUCT {
          ?d a dm:Document .
          ?d dm:docSource ?src .
        } WHERE {
          SELECT ?name ?person WHERE {
            ?person foaf:mbox <mailto:alice@example.org> .
            ?person foaf:name ?name .
            FILTER(?year > 2010)
          }
        }
      """

    QueryConstruct.parse(query) match {
      case Construct(vars,
        bgp,
        Project(Seq(VARIABLE("?name"), VARIABLE("?person")),
        Filter(funcs,expr)),
        List(),
        List()) => succeed
      case _ => fail
    }
  }

  "Query with blank nodes" should "be supported in the QueryConstruct" in {
    val query =
      """
        |PREFIX  dm:   <http://gsk-kg.rdip.gsk.com/dm/1.0/>
        |
        |SELECT ?de ?et
        |
        |WHERE {
        |  ?de dm:predEntityClass _:a .
        |  _:a dm:predClass ?et
        |} LIMIT 10
        |""".stripMargin

    val x = QueryConstruct.parse(query)

    x match {
      case Select(
        mutable.ArrayBuffer(VARIABLE("?de"), VARIABLE("?et")),
        OffsetLimit(
          None,
          Some(10),
          Project(
            mutable.ArrayBuffer(VARIABLE("?de"), VARIABLE("?et")),
            BGP(
              mutable.ArrayBuffer(
                Triple(VARIABLE("?de"),URIVAL("http://gsk-kg.rdip.gsk.com/dm/1.0/predEntityClass"),VARIABLE("??0")),
                Triple(VARIABLE("??0"),URIVAL("http://gsk-kg.rdip.gsk.com/dm/1.0/predClass"),VARIABLE("?et")))))),
        List(),
        List()) =>
        succeed
      case _ => fail
    }

  }
}
