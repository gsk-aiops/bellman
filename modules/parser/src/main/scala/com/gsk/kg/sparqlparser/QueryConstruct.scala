package com.gsk.kg.sparqlparser

import cats.syntax.either._

import org.apache.jena.query.QueryFactory
import org.apache.jena.sparql.algebra.Algebra
import org.apache.jena.sparql.core.{Quad => JenaQuad}

import com.gsk.kg.Graphs
import com.gsk.kg.config.Config
import com.gsk.kg.sparqlparser.EngineError.ParsingError
import com.gsk.kg.sparqlparser.Expr._
import com.gsk.kg.sparqlparser.Query._
import com.gsk.kg.sparqlparser.StringVal._

import scala.collection.JavaConverters._
import org.apache.jena.graph.Node
import java.util.Collection

object QueryConstruct {

  def parse(
      sparql: String,
      config: Config
  ): Result[(Query, Graphs)] = for {
    query <- Either
      .catchNonFatal(QueryFactory.create(sparql))
      .leftMap[EngineError](t => ParsingError(t.getMessage))
    compiled <- Either
      .catchNonFatal(Algebra.compile(query))
      .leftMap[EngineError](t => ParsingError(t.getMessage))
    algebra <- fastparse
      .parse(
        compiled.toString,
        ExprParser.parser(_),
        verboseFailures = true
      )
      .toParserResult
    defaultGraphs =
      if (config.isDefaultGraphExclusive) {
        query.getGraphURIs.asScala.toList
          .map(wrapInAngleBrackets)
          .map(URIVAL) :+ URIVAL("")
      } else {
        Nil
      }
    namedGraphs = query.getNamedGraphURIs.asScala.toList
      .map(wrapInAngleBrackets)
      .map(URIVAL)
    graphs = Graphs(defaultGraphs, namedGraphs)
    result <- query match {
      case q if q.isConstructType =>
        val template = query.getConstructTemplate
        val vars     = getVars(query)
        val bgp      = toBGP(template.getQuads.asScala)
        (Construct(vars, bgp, algebra), graphs).asRight
      case q if q.isSelectType =>
        val vars = getVars(query)
        (Select(vars, algebra), graphs).asRight
      case q if q.isDescribeType =>
        val queryVars: Seq[Node] = query
          .getProjectVars
          .asScala

        val queryUris: Seq[Node] = query
          .getResultURIs
          .asScala

        val vars = (queryVars ++ queryUris)
          .map(node => Quad.jenaNodeToStringVal(node).get)

        (Describe(vars, algebra), graphs).asRight
      case q if q.isAskType =>
        (Ask(algebra), graphs).asRight
      case _ =>
        ParsingError(
          s"The query type: ${query.queryType()} is not supported yet"
        ).asLeft
    }
  } yield result

  private def wrapInAngleBrackets(string: String): String =
    s"<$string>"

  private def getVars(query: org.apache.jena.query.Query): Seq[VARIABLE] =
    query.getProjectVars.asScala.map(v =>
      VARIABLE(v.toString().replace(".", ""))
    )

  def parseADT(sparql: String, config: Config): Result[Expr] =
    parse(sparql, config).map(_._1.r)

  def getAllVariableNames(bgp: BGP): Set[String] = {
    bgp.quads.foldLeft(Set.empty[String]) { (acc, q) =>
      acc ++ Set(q.s, q.p, q.o, q.g).flatMap[String, Set[String]] {
        case VARIABLE(v) => Set(v)
        case _           => Set.empty
      }
    }
  }

  def toBGP(quads: Iterable[JenaQuad]): BGP =
    BGP(quads.flatMap(Quad.toIter).toSeq)
}
