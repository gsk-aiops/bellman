package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.StringVal._
import com.gsk.kg.sparqlparser.Expr._
import com.gsk.kg.sparqlparser.Query._
import fastparse.Parsed.{Failure, Success}
import org.apache.jena.query.QueryFactory
import org.apache.jena.sparql.algebra.Algebra
import org.apache.jena.sparql.core.{Quad => JenaQuad}

import collection.JavaConverters._

object QueryConstruct {

  case class SparqlParsingError(s: String) extends Exception(s)

  def parse(sparql: String): Query = {
    val query = QueryFactory.create(sparql)
    val compiled = Algebra.compile(query)
    val parsed = fastparse.parse(compiled.toString, ExprParser.parser(_), verboseFailures = true)
    val algebra =  parsed match {
      case Success(value, index) => value
      case Failure(str, i, extra) =>
        throw SparqlParsingError(s"$str at position $i, ${extra.input}")
      case _ => //Failure()
        throw SparqlParsingError(s"$sparql parsing failure.")
    }

    val defaultGraphs = query.getGraphURIs.asScala.toList.map(URIVAL)
    val namedGraphs = query.getNamedGraphURIs.asScala.toList.map(URIVAL)

    if (query.isConstructType) {
      val template = query.getConstructTemplate
      val vars = getVars(query)
      val bgp = toBGP(template.getQuads.asScala)
      Construct(vars, bgp, algebra, defaultGraphs, namedGraphs)
    } else if (query.isSelectType) {
      val vars = getVars(query)
      Select(vars, algebra, defaultGraphs, namedGraphs)
    } else if (query.isDescribeType) {
      Describe(getVars(query), algebra, defaultGraphs, namedGraphs)
    } else if (query.isAskType) {
      Ask(algebra, defaultGraphs, namedGraphs)
    } else {
      throw SparqlParsingError(s"The query type: ${query.queryType()} is not supported yet")
    }
  }

  private def getVars(query: org.apache.jena.query.Query): Seq[VARIABLE] = {
    query.getProjectVars.asScala.map(v => VARIABLE(v.toString()))
  }

  def parseADT(sparql: String): Expr = {
    parse(sparql).r
  }

  def getAllVariableNames(bgp: BGP): Set[String] = {
    bgp.quads.foldLeft(Set.empty[String]) {
      (acc, q) =>
        acc ++ Set(q.s, q.p, q.o, q.g).flatMap { e =>
          e match {
            case VARIABLE(v) => Some(v)
//            case GRAPH_VARIABLE => Some(GRAPH_VARIABLE.s)
            case _ => None
          }
        }
    }
  }

  def toBGP(quads: Iterable[JenaQuad]): BGP = {
    BGP(quads.flatMap(Quad(_)).toSeq)
  }
}
