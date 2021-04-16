package com.gsk.kg.sparqlparser

import org.apache.jena.query.QueryFactory
import org.apache.jena.sparql.algebra.Algebra
import org.apache.jena.sparql.core.{Quad => JenaQuad}

import com.gsk.kg.Graphs
import com.gsk.kg.config.Config
import com.gsk.kg.sparqlparser.Expr._
import com.gsk.kg.sparqlparser.Query._
import com.gsk.kg.sparqlparser.StringVal._

import scala.collection.JavaConverters._

import fastparse.Parsed.Failure
import fastparse.Parsed.Success

object QueryConstruct {

  final case class SparqlParsingError(s: String) extends Exception(s)

  def parse(
      sparql: String,
      config: Config
  ): (Query, Graphs) = {
    val query    = QueryFactory.create(sparql)
    val compiled = Algebra.compile(query)
    val parsed = fastparse.parse(
      compiled.toString,
      ExprParser.parser(_),
      verboseFailures = true
    )
    val algebra = parsed match {
      case Success(value, index) => value
      case Failure(str, i, extra) =>
        throw SparqlParsingError(s"$str at position $i, ${extra.input}")
      case _ => //Failure()
        throw SparqlParsingError(s"$sparql parsing failure.")
    }

    val defaultGraphs = if (config.isDefaultGraphExclusive) {
      query.getGraphURIs.asScala.toList.map(URIVAL) :+ URIVAL("")
    } else {
      Nil
    }
    val namedGraphs =
      query.getNamedGraphURIs.asScala.toList.map(URIVAL)
    val graphs = Graphs(defaultGraphs, namedGraphs)

    if (query.isConstructType) {
      val template = query.getConstructTemplate
      val vars     = getVars(query)
      val bgp      = toBGP(template.getQuads.asScala)
      (Construct(vars, bgp, algebra), graphs)
    } else if (query.isSelectType) {
      val vars = getVars(query)
      (Select(vars, algebra), graphs)
    } else if (query.isDescribeType) {
      (Describe(getVars(query), algebra), graphs)
    } else if (query.isAskType) {
      (Ask(algebra), graphs)
    } else {
      throw SparqlParsingError(
        s"The query type: ${query.queryType()} is not supported yet"
      )
    }
  }

  private def getVars(query: org.apache.jena.query.Query): Seq[VARIABLE] =
    query.getProjectVars.asScala.map(v =>
      VARIABLE(v.toString().replace(".", ""))
    )

  def parseADT(sparql: String, config: Config): Expr =
    parse(sparql, config)._1.r

  def getAllVariableNames(bgp: BGP): Set[String] = {
    bgp.quads.foldLeft(Set.empty[String]) { (acc, q) =>
      acc ++ Set(q.s, q.p, q.o, q.g).flatMap[String, Set[String]] {
        case VARIABLE(v) => Set(v)
        case _           => Set.empty
      }
    }
  }

  def toBGP(quads: Iterable[JenaQuad]): BGP =
    BGP(quads.flatMap(Quad.toIter(_)).toSeq)
}
