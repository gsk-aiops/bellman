package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.Expr._
import com.gsk.kg.sparqlparser.Query._
import com.gsk.kg.sparqlparser.StringVal._

import scala.collection.mutable

import org.scalatest.flatspec.AnyFlatSpec

class QueryConstructSpec extends AnyFlatSpec with TestUtils with TestConfig {

  "Simple Query" should "parse Construct statement with correct number of Triples" in {
    query("/queries/q0-simple-basic-graph-pattern.sparql") match {
      case Construct(vars, bgp, expr) =>
        assert(vars.size == 2 && bgp.quads.size == 2)
      case _ => fail
    }
  }

  "Construct" should "result in proper variables, a basic graph pattern, and algebra expression" in {
    query("/queries/q3-union.sparql") match {
      case Construct(
            vars,
            bgp,
            Union(BGP(quadsL: Seq[Quad]), BGP(quadsR: Seq[Quad]))
          ) =>
        val temp = QueryConstruct.getAllVariableNames(bgp)
        val all  = vars.map(_.s).toSet
        assert((all -- temp) == Set("?lnk"))
      case _ => fail
    }
  }

  "Construct with Bind" should "contains bind variable" in {
    query("/queries/q4-simple-bind.sparql") match {
      case Construct(
            vars,
            bgp,
            Extend(l: StringVal, r: StringVal, BGP(quads: Seq[Quad]))
          ) =>
        vars.exists(_.s == "?dbind")
      case _ => fail
    }
  }

  "Complex named graph query" should "be captured properly in Construct" in {
    query("/queries/q13-complex-named-graph.sparql") match {
      case Construct(vars, bgp, expr) =>
        assert(vars.size == 13)
        assert(vars.exists(va => va.s == "?ogihw"))
      case _ => fail
    }
  }

  "Complex lit-search query" should "return proper Construct type" in {
    val a = 1
    query("/queries/lit-search-3.sparql") match {
      case Construct(vars, bgp, expr) =>
        assert(bgp.quads.size == 11)
        assert(
          bgp.quads.head.o
            .asInstanceOf[BLANK]
            .s == bgp.quads(1).s.asInstanceOf[BLANK].s
        )
        assert(vars.exists(v => v.s == "?secid"))
      case _ => fail
    }
  }

  "Extra large query" should "return proper Construct type" in {
    query("/queries/lit-search-xlarge.sparql") match {
      case Construct(vars, bgp, expr) =>
        assert(bgp.quads.size == 67)
        assert(bgp.quads.head.s.asInstanceOf[VARIABLE].s == "?Year")
        assert(bgp.quads.last.s.asInstanceOf[VARIABLE].s == "?Predication")
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

    parse(query) match {
      case (
            Construct(
              vars,
              bgp,
              Project(
                Seq(VARIABLE("?name"), VARIABLE("?person")),
                Filter(funcs, expr)
              )
            ),
            _
          ) =>
        succeed
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

    val x = parse(query)

    x match {
      case (
            Select(
              mutable.ArrayBuffer(VARIABLE("?de"), VARIABLE("?et")),
              OffsetLimit(
                None,
                Some(10),
                Project(
                  mutable.ArrayBuffer(VARIABLE("?de"), VARIABLE("?et")),
                  BGP(
                    mutable.ArrayBuffer(
                      Quad(
                        VARIABLE("?de"),
                        URIVAL(
                          "http://gsk-kg.rdip.gsk.com/dm/1.0/predEntityClass"
                        ),
                        VARIABLE("??0"),
                        _
                      ),
                      Quad(
                        VARIABLE("??0"),
                        URIVAL("http://gsk-kg.rdip.gsk.com/dm/1.0/predClass"),
                        VARIABLE("?et"),
                        _
                      )
                    )
                  )
                )
              )
            ),
            _
          ) =>
        succeed
      case _ => fail
    }

  }
}
