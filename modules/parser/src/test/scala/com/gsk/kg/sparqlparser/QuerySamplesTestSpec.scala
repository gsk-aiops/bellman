package com.gsk.kg.sparqlparser
import com.gsk.kg.sparqlparser.Expr._
import com.gsk.kg.sparqlparser.Query.{Construct, Describe, Select}
import com.gsk.kg.sparqlparser.BuildInFunc._
import com.gsk.kg.sparqlparser.StringVal._
import org.apache.jena.query.QueryFactory
import org.apache.jena.sparql.algebra.{Algebra, Op}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class QuerySamplesTestSpec extends AnyFlatSpec with Matchers {

  def showAlgebra(q: String): Op = {
    val query = QueryFactory.create(q)
    Algebra.compile(query)
  }

  "Get a small sample" should "parse" in {
    val query = QuerySamples.q1
    val expr = QueryConstruct.parseADT(query)
    expr match {
      case OffsetLimit(None,Some(20), _) => succeed
      case _ =>
        fail
    }
  }

  "Find label" should "parse" in {
    val query = QuerySamples.q2
    val expr = QueryConstruct.parseADT(query)
    expr match {
      case Project(vs, BGP(_)) =>
        assert(vs.nonEmpty && vs.head == VARIABLE("?label"))
      case _ =>
        fail
    }
  }

  "Find distinct label" should "parse" in {
    val query = QuerySamples.q3
    val expr = QueryConstruct.parseADT(query)
    expr match {
      case Distinct(Project(vs, BGP(_))) =>
        assert(vs.nonEmpty && vs.head == VARIABLE("?label"))
      case _ =>
        fail
    }
  }

  "Get all relations" should "parse" in {
    val query = QuerySamples.q4
    val expr = QueryConstruct.parseADT(query)
    expr match {
      case Project(vs, BGP(_)) =>
        assert(vs.nonEmpty && vs.size == 2)
      case _ =>
        fail
    }
  }

  "Get parent class" should "parse" in {
    val query = QuerySamples.q5
    val expr = QueryConstruct.parseADT(query)
    expr match {
      case Project(vs, BGP(_)) =>
        assert(vs.nonEmpty && vs.head == VARIABLE("?parent"))
      case _ =>
        fail
    }
  }

  "Get parent class with filter" should "parse" in {
    val query = QuerySamples.q6
    val expr = QueryConstruct.parseADT(query)
    expr match {
      case Project(vs, Filter(funcs,r)) =>
        assert(vs.nonEmpty && vs.head == VARIABLE("?parent"))
      case _ =>
        fail
    }
  }

  "Test multiple hops" should "parse" in {
    val query = QuerySamples.q7
    val expr = QueryConstruct.parseADT(query)
    expr match {
      case Project(vs, BGP(triples)) =>
        assert(vs.nonEmpty && vs.head == VARIABLE("?species"))
        assert(triples.size==7)
      case _ =>
        fail
    }
  }

  "Test multiple hops and prefixes" should "parse" in {
    val query = QuerySamples.q8
    val expr = QueryConstruct.parseADT(query)
    expr match {
      case Project(vs, BGP(triples)) =>
        assert(vs.nonEmpty && vs.head == VARIABLE("?species"))
        assert(triples.size==7)
      case _ =>
        fail
    }
  }

  "Test find label" should "parse" in {
    val query = QuerySamples.q9
    val expr = QueryConstruct.parseADT(query)
    expr match {
      case Project(vs, BGP(triples)) =>
        assert(vs.nonEmpty && vs.head == VARIABLE("?label"))
      case _ =>
        fail
    }
  }

  "Test find parent class" should "parse" in {
    val query = QuerySamples.q10
    val expr = QueryConstruct.parseADT(query)
    expr match {
      case Project(vs, BGP(triples)) =>
        assert(vs.nonEmpty && vs.head == VARIABLE("?parent"))
        assert(triples.size == 1)
      case _ =>
        fail
    }
  }


  "Tests hops and distinct" should "parse" in {
    val query = QuerySamples.q11
    val expr = QueryConstruct.parseADT(query)
    expr match {
      case Distinct(Project(vs, BGP(triples))) =>
        assert(vs.nonEmpty && vs.head == VARIABLE("?parent_name"))
        assert(triples.size == 2)
      case _ =>
        fail
    }
  }

  "Tests filter and bind" should "parse" in {
    val query = QuerySamples.q12
    val expr = QueryConstruct.parseADT(query)
    expr match {
      case Project(vs, Filter(funcs,Extend(to,from,BGP(_)))) =>
        assert(vs.nonEmpty && vs.size == 3)
        assert(funcs.size == 1)
        assert(to == VARIABLE("?o"))
      case _ =>
        fail
    }
  }

  // ignore for now since for diff position, Jena generates different representations
  "Test BIND in another position in the query" should "parse to same as q12" in {
    val query = QuerySamples.q13
    val expr = QueryConstruct.parseADT(query)
    expr match {
      case Project(vs, Filter(funcs, Join(Extend(to,from,TabUnit()),BGP(_)))) =>
        assert(vs.nonEmpty && vs.size == 3)
        assert(funcs.size == 1)
        assert(to == VARIABLE("?o"))
      case _ =>
        fail
    }
  }

  "Test union" should "parse" in {
    val query = QuerySamples.q14
    val expr = QueryConstruct.parseADT(query)
    expr match {
      case Project(vs, Union(l,r)) =>
        assert(vs.nonEmpty && vs.size == 3)
        r match {
          case Filter(funcs,e) => succeed
          case _ => fail
        }
      case _ =>
        fail
    }
  }

  "Test simple describe query" should "parse" in {
    val query = QuerySamples.q15
    val q = QueryConstruct.parse(query)
    q match {
      case Describe(_, OpNil(), List(), List()) =>
        succeed
      case _ =>
        fail
    }
  }

  "Test describe query" should "parse" in {
    val query = QuerySamples.q16
    val q = QueryConstruct.parse(query)
    q match {
      case Describe(vars, Project(vs, Filter(_,_)), List(), List()) =>
        assert(vars == vs)
        succeed
      case _ =>
        fail
    }
  }

  "Tests str conversion and logical operators" should "parse" in {
    val query = QuerySamples.q17
    val q = QueryConstruct.parse(query)
    q match {
      case Select(vars, Project(vs, Filter(_,_)), List(), List()) =>
        succeed
      case _ =>
        fail
    }
  }

  "Tests FILTER positioning with graph sub-patterns" should "parse" in {
    val query = QuerySamples.q18
    val q = QueryConstruct.parse(query)
    q match {
      case Select(vars, Project(vs, Filter(_,_)), List(), List()) =>
        succeed
      case _ =>
        fail
    }
  }

  "Test for FILTER in different positions" should "parse" in {
    val query = QuerySamples.q19
    val q = QueryConstruct.parse(query)
    q match {
      case Select(vars, Distinct(Project(vs, Filter(_,_))), List(), List()) =>
        succeed
      case _ =>
        fail
    }
  }

  "Test CONSTRUCT and string replacement" should "parse" in {
    val query = QuerySamples.q20
    val q = QueryConstruct.parse(query)
    q match {
      case Construct(vars, _, Extend(to,REPLACE(_,_,_),r), List(), List()) =>
        succeed
      case _ =>
        fail
    }
  }

  "Test document query" should "parse" in {
    val query = QuerySamples.q21
    val q = QueryConstruct.parse(query)
    q match {
      case Select(vars, Project(vs, BGP(ts)), List(), List()) =>
        assert(ts.size == 21)
      case _ =>
        fail
    }
  }

  //Comprehensive queries

  "Get a sample of triples joining non-blank nodes" should "parse" in {
    val query = QuerySamples.q22
    val q = QueryConstruct.parse(query)
    q match {
      case Select(vars, OffsetLimit(None, Some(10),Project(_,Filter(_,_))), List(), List()) =>
        succeed
      case _ =>
        fail
    }
  }

  "Check DISTINCT works" should "parse" in {
    val query = QuerySamples.q23
    val q = QueryConstruct.parse(query)
    q match {
      case Select(vars, OffsetLimit(None, Some(10),Distinct(Project(vs,_))), List(), List()) =>
        assert(vs.size == 3)
      case _ =>
        fail
    }
  }

  "Get class parent-child relations" should "parse" in {
    val query = QuerySamples.q24
    val q = QueryConstruct.parse(query)
    q match {
      case Select(vars, Project(_,Filter(_,_)), List(), List()) =>
        succeed
      case _ =>
        fail
    }
  }

  "Get class parent-child relations with optional labels" should "parse" in {
    val query = QuerySamples.q25
    val q = QueryConstruct.parse(query)
    q match {
      case Select(vars, Project(_,Filter(_,_)), List(), List()) =>
        succeed
      case _ =>
        fail
    }
  }

  "Get all labels in file" should "parse" in {
    val query = QuerySamples.q26
    val q = QueryConstruct.parse(query)
    q match {
      case Select(vars, Distinct(Project(_,_)), List(), List()) =>
        succeed
      case _ =>
        fail
    }
  }

  "Get label of owl:Thing" should "parse" in {
    val query = QuerySamples.q27
    val q = QueryConstruct.parse(query)
    q match {
      case Select(vars, Project(vs,_), List(), List()) =>
        assert(vs.nonEmpty && vs.head == VARIABLE("?label"))
      case _ =>
        fail
    }
  }

  "Get label of owl:Thing with prefix" should "parse" in {
    val query = QuerySamples.q28
    val q = QueryConstruct.parse(query)
    q match {
      case Select(vars, Project(_,_), List(), List()) =>
        succeed
      case _ =>
        fail
    }
  }

  "Get label of owl:Thing with explanatory comment" should "parse" in {
    val query = QuerySamples.q29
    val q = QueryConstruct.parse(query)
    q match {
      case Select(vars, Project(_,_), List(), List()) =>
        succeed
      case _ =>
        fail
    }
  }

  "Get label of owl:Thing with regex to remove poor label if present" should "parse" in {
    val query = QuerySamples.q30
    val q = QueryConstruct.parse(query)
    q match {
      case Select(vars, Project(_,Filter(_,_)), List(), List()) =>
        succeed
      case _ =>
        fail
    }
  }

  "Construct a graph where everything which is a Thing is asserted to exist" should "parse" in {
    val query = QuerySamples.q31
    val q = QueryConstruct.parse(query)
    q match {
      case Construct(vars, bgp, BGP(_), List(), List()) =>
        succeed
      case _ =>
        fail
    }
  }

  "Construct a graph where all the terms derived from a species have a new relation" should "parse" in {
    val query = QuerySamples.q32
    val q = QueryConstruct.parse(query)
    q match {
      case Construct(vars, bgp, BGP(_), List(), List()) =>
        succeed
      case _ =>
        fail
    }
  }

  "Detect punned relations in an ontology" should "parse" in {
    val query = QuerySamples.q33
    val q = QueryConstruct.parse(query)
    q match {
      case Select(vars, Project(_,_), List(), List()) =>
        succeed
      case _ =>
        fail
    }
  }

  "Construct a triple where the predicate is derived" should "parse" in {
    val query = QuerySamples.q34
    val q = QueryConstruct.parse(query)
    q match {
      case Construct(vars, bgp, Union(BGP(_),BGP(_)), List(), List())=>
        succeed
      case _ =>
        fail
    }
  }

  "Query to convert schema of predications" should "parse" in {
    val query = QuerySamples.q35
    val q = QueryConstruct.parse(query)
    q match {
      case Construct(vars, bgp, Extend(to, from, e), List(), List())=>
        assert(to == VARIABLE("?pred"))
      case _ =>
        fail
    }
  }

  "Query with default and named graphs to match on specific graph" should "parse" in {
    val query = QuerySamples.q36
    val q = QueryConstruct.parse(query)
    q match {
      case Select(_, Project(_, Graph(specifiedGraph, _)), defaultGraphs, namedGraphs) =>
        defaultGraphs should (have size 1 and contain(
          URIVAL("http://example.org/dft.ttl")))
        namedGraphs should (have size 2 and contain allElementsOf
          List(URIVAL("http://example.org/alice"), URIVAL("http://example.org/bob")))
        specifiedGraph shouldEqual namedGraphs.head
      case _ =>
        fail
    }
  }


  "Query with default and named graphs to match on multiple graphs" should "parse" in {
    val query = QuerySamples.q37
    val q = QueryConstruct.parse(query)
    q match {
      case Select(vars, Project(_, Join(_, Graph(graphVariable, _))), defaultGraphs, namedGraphs) =>
        defaultGraphs should (have size 1 and contain(
          URIVAL("http://example.org/dft.ttl")))
        namedGraphs should (have size 2 and contain allElementsOf
          List(URIVAL("http://example.org/alice"), URIVAL("http://example.org/bob")))
        graphVariable shouldEqual vars(1)
      case _ =>
        fail
    }
  }

}
