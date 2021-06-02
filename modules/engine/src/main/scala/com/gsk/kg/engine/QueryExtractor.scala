package com.gsk.kg.engine

import higherkindness.droste.Algebra

import com.gsk.kg.config.Config
import com.gsk.kg.sparqlparser.Expr.fixedpoint._
import com.gsk.kg.sparqlparser.QueryConstruct

import java.net.URI

object QueryExtractor {

  final case class QueryParam(param: String, value: String)

  def extractInfo(query: String): (String, Map[String, List[QueryParam]]) = {
    val (_, graphs) = QueryConstruct.parse(query, Config.default).right.get

    ("", (graphs.default ++ graphs.named)
      .filterNot(_.s.isEmpty)
      .map(s => new URI(s.s.stripPrefix("<").stripSuffix(">")))
      .map(uri => {
        val params = extractQueryParams(uri)
        val cleanUri = new URI(
          uri.getScheme, uri.getUserInfo, uri.getHost, uri.getPort, uri.getPath, null, null)

        (cleanUri.toString, params)
      }) 
      .toMap)
  }

  
  private val exprToString: Algebra[ExprF, String] =
    Algebra {
      case ExtendF(bindTo, bindFrom, r) => ??? 
      case FilteredLeftJoinF(l, r, f)   => ??? 
      case UnionF(l, r)                 => ??? 
      case BGPF(quads)                  => ??? 
      case OpNilF()                     => ??? 
      case GraphF(g, e)                 => ??? 
      case JoinF(l, r)                  => ??? 
      case LeftJoinF(l, r)              => ??? 
      case ProjectF(vars, r)            => ??? 
      case QuadF(s, p, o, g)            => ??? 
      case DistinctF(r)                 => ??? 
      case GroupF(vars, func, r)        => ??? 
      case OrderF(conds, r) => ???
      case OffsetLimitF(None, None, r)       => ??? 
      case OffsetLimitF(None, Some(l), r)    => ??? 
      case OffsetLimitF(Some(o), None, r)    => ??? 
      case OffsetLimitF(Some(o), Some(l), r) => ??? 
      case FilterF(funcs, expr) => ???
      case TableF(vars, rows) => ???
      case RowF(tuples)       => ???
      case TabUnitF()         => ???
   }

  private def extractQueryParams(uri: URI): List[QueryParam] =
    uri.getQuery.split("&")
      .map(qp => {
        val Array(param, value) = qp.split("=")
        QueryParam(param, value)
      })
      .toList

}
