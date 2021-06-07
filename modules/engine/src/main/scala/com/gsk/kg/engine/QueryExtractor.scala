package com.gsk.kg.engine

import higherkindness.droste._

import com.gsk.kg.config.Config
import com.gsk.kg.engine.ExpressionF.{VARIABLE => _, _}
import com.gsk.kg.sparqlparser.Expr
import com.gsk.kg.sparqlparser.Expr.fixedpoint._
import com.gsk.kg.sparqlparser.Expression
import com.gsk.kg.sparqlparser.Query
import com.gsk.kg.sparqlparser.QueryConstruct
import com.gsk.kg.sparqlparser.StringVal
import com.gsk.kg.sparqlparser.StringVal._

import java.net.URI
import com.gsk.kg.Graphs

object QueryExtractor {

  final case class QueryParam(param: String, value: String)

  def extractInfo(q: String): (String, Map[String, List[QueryParam]]) = {
    val (query, graphs) = QueryConstruct.parse(q, Config.default) match {
      case Left(a) => throw new Exception(a.toString)
      case Right(b) => b
    }

    (
      queryToString(query, graphs),
      (graphs.default ++ graphs.named)
        .filterNot(_.s.isEmpty)
        .map { uriVal =>
          val uriString = uriVal.s.stripPrefix("<").stripSuffix(">")
          val uri = new URI(uriString)
          val params = extractQueryParams(uri)

          (getCleanUri(uriVal), params)
        }
        .toMap
    )
  }

  private def getCleanUri(uriVal: StringVal): String = {
    val uriString = uriVal.s.stripPrefix("<").stripSuffix(">")
    val uri = new URI(uriString)
    new URI(
      uri.getScheme,
      uri.getUserInfo,
      uri.getHost,
      uri.getPort,
      uri.getPath,
      null,
      null
    ).toString
  }

  private def printQuad(quad: Expr.Quad): String =
    s"${quad.s.s} ${quad.p.s} ${quad.o.s} ."

  private def printVars(vars: Seq[VARIABLE]) =
    vars.map(_.s).mkString("", " ", ".")

  private def queryToString(query: Query, graphs: Graphs): String = {
    val toString = scheme.cata(exprToString)

    val g = (
      graphs.default.filterNot(_.s.isEmpty).map(g => s"FROM <${getCleanUri(g)}>") ++ 
      graphs.named.filterNot(_.s.isEmpty).map(g => s"FROM NAMED <${getCleanUri(g)}>"))
      .mkString("\n", "\n", "\n")

    query match {
      case Query.Describe(vars, r) =>
        s"DESCRIBE ${printVars(vars)} $g WHERE { ${toString(r)} }"
      case Query.Ask(r) =>
        s"ASK $g { ${toString(r)} }"
      case Query.Construct(vars, bgp, r) =>
        s"CONSTRUCT { ${bgp.quads.map(printQuad).mkString(" ")} }$g WHERE { ${toString(r)} }"
      case Query.Select(vars, r) =>
        s"SELECT ${printVars(vars)}$g WHERE { ${toString(r)} }"
    }
  }

  private def printExpression(expression: Expression): String =
    scheme.cata[ExpressionF, Expression, String](expressionToString).apply(expression)

  private val expressionToString: Algebra[ExpressionF, String] = 
    Algebra {
        case REGEX(s, pattern, flags) => s"REGEX($s, $pattern, $flags)"
        case REPLACE(st, pattern, by, flags) => s"REPLACE($st, $pattern, $by, $flags)"
        case STRENDS(s, f) => s"STRENDS($s, $f)"
        case STRSTARTS(s, f) => s"STRSTARTS($s, $f)"
        case STRDT(s, uri) => s"STRDT($s, $uri)"
        case STRAFTER(s, f) => s"""STRAFTER($s, "$f")"""
        case STRBEFORE(s, f) => s"STRBEFORE($s, $f)"
        case SUBSTR(s, pos, len) => s"SUBSTR($s, $pos, $len)"
        case STRLEN(s) => s"STRLEN($s)"
        case EQUALS(l, r) => s"EQUALS($l, $r)"
        case GT(l, r) => s"GT($l, $r)"
        case LT(l, r) => s"LT($l, $r)"
        case GTE(l, r) => s"GTE($l, $r)"
        case LTE(l, r) => s"LTE($l, $r)"
        case OR(l, r) => s"OR($l, $r)"
        case AND(l, r) => s"AND($l, $r)"
        case NEGATE(s) => s"NEGATE($s)"
        case ExpressionF.URI(s) => s"URI($s)"
        case LANG(s) => s"LANG($s)"
        case LANGMATCHES(s, range) => s"LANGMATCHES($s, $range)"
        case LCASE(s) => s"LCASE($s)"
        case UCASE(s) => s"UCASE($s)"
        case ISLITERAL(s) => s"ISLITERAL($s)"
        case CONCAT(appendTo, append) => s"CONCAT($appendTo, $append)"
        case STR(s) => s"STR($s)"
        case ISBLANK(s) => s"ISBLANK($s)"
        case ISNUMERIC(s) => s"ISNUMERIC($s)"
        case COUNT(e) => s"COUNT($e)"
        case SUM(e) => s"SUM($e)"
        case MIN(e) => s"MIN($e)"
        case MAX(e) => s"MAX($e)"
        case AVG(e) => s"AVG($e)"
        case SAMPLE(e) => s"SAMPLE($e)"
        case GROUP_CONCAT(e, separator) => s"GROUP_CONCAT($e, $separator)"
        case ENCODE_FOR_URI(s) => s"ENCODE_FOR_URI($s)"
        case MD5(s) => s"MD5($s)"
        case SHA1(s) => s"SHA1($s)"
        case SHA256(s) => s"SHA256($s)"
        case SHA384(s) => s"SHA384($s)"
        case SHA512(s) => s"SHA512($s)"
        case ExpressionF.STRING(s) => s"ExpressionF.STRING($s)"
        case ExpressionF.DT_STRING(s, tag) => s"ExpressionF.DT_STRING($s, $tag)"
        case ExpressionF.LANG_STRING(s, tag) => s"ExpressionF.LANG_STRING($s, $tag)"
        case ExpressionF.NUM(s) => s"ExpressionF.NUM($s)"
        case ExpressionF.VARIABLE(s) => s
        case ExpressionF.URIVAL(s) => s
        case ExpressionF.BLANK(s) => s
        case ExpressionF.BOOL(s) => s
        case ASC(e) => s"ASC($e)"
        case DESC(e) => s"DESC($e)"
    }

  private val exprToString: Algebra[ExprF, String] =
    Algebra {
      case ExtendF(bindTo, bindFrom, r)      => s"$r BIND(${printExpression(bindFrom)} as ${bindTo.s})"
      case FilteredLeftJoinF(l, r, f)        => s"{ $l } OPTIONAL { $r FILTER(${f.map(printExpression).mkString(", ")}) }"
      case UnionF(l, r)                      => s"{ $l } UNION { $r }"
      case BGPF(quads)                       => quads.map(printQuad).mkString(" ")
      case GraphF(g, e)                      => s"GRAPH <${getCleanUri(g)}> { $e }"
      case JoinF(l, r)                       => s"{ $l } JOIN { $r }"
      case LeftJoinF(l, r)                   => s"{ $l } OPTIONAL { $r }"
      case ProjectF(vars, r)                 => r
      case QuadF(s, p, o, g)                 => s"QuadF(s, p, o, g)"
      case DistinctF(r)                      => s"$r DISTINCT"
      case GroupF(vars, func, r)             => s"$r GROUP BY ${printVars(vars)}"
      case OrderF(conds, r)                  => s"$r ORDER BY ${conds.asInstanceOf[Seq[Expression]].map(printExpression).mkString(" ")}"
      case OffsetLimitF(None, None, r)       => s"OffsetLimitF(None, None, r)"
      case OffsetLimitF(None, Some(l), r)    => s"OffsetLimitF(None, Some(l)"
      case OffsetLimitF(Some(o), None, r)    => s"OffsetLimitF(Some(o)"
      case OffsetLimitF(Some(o), Some(l), r) => s"OffsetLimitF(Some(o)"
      case FilterF(funcs, expr)              => s"$expr FILTER(${funcs.map(printExpression).mkString(" ")})"
      case TableF(vars, rows)                => s"TableF(vars, rows)"
      case RowF(tuples)                      => s"RowF(tuples)"
      case TabUnitF()                        => s"TabUnitF()"
      case MinusF(l, r) => s"{ $l } MINUS { $r }"
      case OpNilF()     => s"OpNilF()"
      case ExistsF(not, p, r)                => 
        val n = if (not) "NOT" else ""
        s"$r $n EXISTS { $p }"
    }

  private def extractQueryParams(uri: URI): List[QueryParam] =
    uri.getQuery
      .split("&")
      .map { qp =>
        val Array(param, value) = qp.split("=")
        QueryParam(param, value)
      }
      .toList

}
