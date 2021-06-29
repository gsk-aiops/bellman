package com.gsk.kg.engine

import higherkindness.droste._

import org.apache.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.graph.Node
import org.apache.jena.graph.NodeFactory
import org.apache.jena.graph.{Triple => JenaTriple}
import org.apache.jena.sparql.algebra.OpAsQuery
import org.apache.jena.sparql.core.BasicPattern
import org.apache.jena.sparql.sse.SSE
import org.apache.jena.sparql.syntax.Template

import com.gsk.kg.Graphs
import com.gsk.kg.config.Config
import com.gsk.kg.engine.ExpressionF.{VARIABLE => _, _}
import com.gsk.kg.sparqlparser.Expr
import com.gsk.kg.sparqlparser.Expr.fixedpoint._
import com.gsk.kg.sparqlparser.Expression
import com.gsk.kg.sparqlparser.Query
import com.gsk.kg.sparqlparser.QueryConstruct
import com.gsk.kg.sparqlparser.StringVal

import java.net.URI
import java.{util => ju}

import scala.collection.JavaConverters._

object QueryExtractor {

  final case class QueryParam(param: String, value: String)

  def extractInfo(q: String): (String, Map[String, List[QueryParam]]) = {
    val (query, graphs) = QueryConstruct.parse(q, Config.default) match {
      case Left(a)  => throw new Exception(a.toString)
      case Right(b) => b
    }

    (
      queryToString(query, graphs),
      (graphs.default ++ graphs.named ++ getGraphs(query.r))
        .filterNot(_.s.isEmpty)
        .map { uriVal =>
          val uriString = stripUriFromAngleBrackets(uriVal.s)
          val uri       = new URI(uriString)
          val params    = extractQueryParams(uri)

          (getCleanUri(uriVal), params)
        }
        .toMap
    )
  }

  private def isKnowledgeGraph(graphUri: String): Boolean =
    graphUri.contains("/kg?")

  private def getGraphs(expr: Expr): List[StringVal] = {
    val extractGraphs: Algebra[ExprF, List[StringVal]] = Algebra {
      case ExtendF(bindTo, bindFrom, r)      => r
      case FilteredLeftJoinF(l, r, f)        => l ++ r
      case UnionF(l, r)                      => l ++ r
      case BGPF(quads)                       => Nil
      case GraphF(g, e)                      => g :: e
      case JoinF(l, r)                       => l ++ r
      case LeftJoinF(l, r)                   => l ++ r
      case ProjectF(vars, r)                 => r
      case QuadF(s, p, o, g)                 => Nil
      case DistinctF(r)                      => r
      case ReducedF(r)                       => r
      case GroupF(vars, func, r)             => r
      case OrderF(conds, r)                  => r
      case OffsetLimitF(None, None, r)       => r
      case OffsetLimitF(None, Some(l), r)    => r
      case OffsetLimitF(Some(o), None, r)    => r
      case OffsetLimitF(Some(o), Some(l), r) => r
      case FilterF(funcs, expr)              => expr
      case TableF(vars, rows)                => Nil
      case RowF(tuples)                      => Nil
      case TabUnitF()                        => Nil
      case MinusF(l, r)                      => l ++ r
      case OpNilF()                          => Nil
      case ExistsF(not, p, r)                => r
    }

    val fn = scheme.cata(extractGraphs)

    fn(expr)
  }

  private def getCleanUri(uriVal: StringVal): String = {
    val uriString = stripUriFromAngleBrackets(uriVal.s)

    if (isKnowledgeGraph(uriString)) {
      ""
    } else {
      val uri = new URI(uriString)
      new URI(
        uri.getScheme,
        uri.getUserInfo,
        uri.getHost,
        uri.getPort,
        uri.getPath,
        null, // scalastyle:off
        null  // scalastyle:off
      ).toString
    }
  }

  private def printQuad(quad: Expr.Quad): String =
    s"(triple ${quad.s.s} ${quad.p.s} ${quad.o.s})"

  private def printStringVal(vars: Seq[StringVal]) =
    vars.map(_.s).mkString(" ")

  private def queryToString(query: Query, graphs: Graphs): String = {
    val toString = scheme.cata(exprToString)

    val jenaQuery = query match {
      case Query.Describe(vars, r) =>
        val query = OpAsQuery.asQuery(SSE.parseOp(toString(r)))

        query.setQueryDescribeType()

        query
      case Query.Ask(r) =>
        val query = OpAsQuery.asQuery(SSE.parseOp(toString(r)))

        query.setQueryAskType()

        query
      case Query.Construct(vars, bgp, r) =>
        val query = OpAsQuery.asQuery(SSE.parseOp(toString(r)))

        query.setQueryConstructType()
        query.setConstructTemplate(getConstructTemplate(bgp))

        query
      case Query.Select(vars, r) =>
        OpAsQuery.asQuery(SSE.parseOp(toString(r)))
    }

    graphs.default
      .map(getCleanUri)
      .filterNot(_.isEmpty)
      .foreach { g =>
        jenaQuery.addGraphURI(stripUriFromAngleBrackets(g))
      }

    graphs.named
      .map(getCleanUri)
      .filterNot(_.isEmpty)
      .foreach { g =>
        jenaQuery.addNamedGraphURI(stripUriFromAngleBrackets(g))
      }

    jenaQuery.serialize()
  }

  private def stripUriFromAngleBrackets(string: String): String =
    string.stripPrefix("<").stripSuffix(">")

  private def toNode(sv: StringVal): Node =
    sv match {
      case StringVal.STRING(s)            => NodeFactory.createLiteral(s)
      case StringVal.DT_STRING(s, tag)    => NodeFactory.createLiteral(s)
      case StringVal.LANG_STRING(s, lang) => NodeFactory.createLiteral(s, lang)
      case StringVal.NUM(s) =>
        NodeFactory.createLiteral(s, XSDDatatype.XSDdecimal)
      case StringVal.VARIABLE(s) =>
        NodeFactory.createVariable(s.stripPrefix("?"))
      case StringVal.URIVAL(s) =>
        NodeFactory.createURI(stripUriFromAngleBrackets(s))
      case StringVal.BLANK(s) => NodeFactory.createBlankNode(s)
      case StringVal.BOOL(s) =>
        NodeFactory.createLiteral(s, XSDDatatype.XSDboolean)
      case _ => NodeFactory.createBlankNode()
    }

  private def getConstructTemplate(bgp: Expr.BGP): Template = {
    val triples: ju.List[JenaTriple] =
      bgp.quads
        .map(q => new JenaTriple(toNode(q.s), toNode(q.p), toNode(q.o)))
        .toList
        .asJava

    val pattern = BasicPattern.wrap(triples)

    new Template(
      pattern
    )
  }

  private def printExpression(expression: Expression): String =
    scheme
      .cata[ExpressionF, Expression, String](expressionToString)
      .apply(expression)

  private val expressionToString: Algebra[ExpressionF, String] =
    Algebra {
      case ADD(l, r)                => s"(add $l $r)"
      case SUBTRACT(l, r)           => s"(subtract $l $r)"
      case MULTIPLY(l, r)           => s"(multiply $l $r)"
      case DIVIDE(l, r)             => s"(divide $l $r)"
      case REGEX(s, pattern, flags) => s"(regex $s $pattern $flags)"
      case REPLACE(st, pattern, by, flags) =>
        s"(replace $st $pattern $by $flags)"
      case STRENDS(s, f)                   => s"(strends $s $f)"
      case STRSTARTS(s, f)                 => s"(strstarts $s $f)"
      case STRDT(s, uri)                   => s"(strdt $s $uri)"
      case STRAFTER(s, f)                  => s"""(strafter $s "$f")"""
      case STRBEFORE(s, f)                 => s"(strbefore $s $f)"
      case SUBSTR(s, pos, len)             => s"(substr $s $pos $len)"
      case STRLEN(s)                       => s"(strlen $s)"
      case EQUALS(l, r)                    => s"(equals $l $r)"
      case GT(l, r)                        => s"$l > $r"
      case LT(l, r)                        => s"$l < $r"
      case GTE(l, r)                       => s"$l >= $r"
      case LTE(l, r)                       => s"$l <= $r"
      case OR(l, r)                        => s"(or $l $r)"
      case AND(l, r)                       => s"(and $l $r)"
      case NEGATE(s)                       => s"(negate $s)"
      case IN(e, xs)                       => s"(in $e ${xs.mkString(", ")})"
      case SAMETERM(l, r)                  => s"(sameterm $l $r)"
      case IF(cnd, ifTrue, ifFalse)        => s"(if $cnd $ifTrue $ifFalse)"
      case BOUND(e)                        => s"(bound $e)"
      case COALESCE(xs)                    => s"(coalesce ${xs.mkString(", ")})"
      case ExpressionF.URI(s)              => s"(uri $s)"
      case LANG(s)                         => s"(lang $s)"
      case LANGMATCHES(s, range)           => s"(langmatches $s $range)"
      case LCASE(s)                        => s"(lcase $s)"
      case UCASE(s)                        => s"(ucase $s)"
      case ISLITERAL(s)                    => s"(isliteral $s)"
      case CONCAT(appendTo, append)        => s"(concat $appendTo $append)"
      case STR(s)                          => s"(str $s)"
      case ISBLANK(s)                      => s"(isblank $s)"
      case ISNUMERIC(s)                    => s"(isnumeric $s)"
      case COUNT(e)                        => s"(count $e)"
      case SUM(e)                          => s"(sum $e)"
      case MIN(e)                          => s"(min $e)"
      case MAX(e)                          => s"(max $e)"
      case AVG(e)                          => s"(avg $e)"
      case SAMPLE(e)                       => s"(sample $e)"
      case GROUP_CONCAT(e, separator)      => s"(group_concat $e $separator)"
      case ENCODE_FOR_URI(s)               => s"(encode_for_uri $s)"
      case MD5(s)                          => s"(md5 $s)"
      case SHA1(s)                         => s"(sha1 $s)"
      case SHA256(s)                       => s"(sha256 $s)"
      case SHA384(s)                       => s"(sha384 $s)"
      case SHA512(s)                       => s"(sha512 $s)"
      case ExpressionF.STRING(s)           => s"""\"$s\""""
      case ExpressionF.DT_STRING(s, tag)   => s"""\"$s\"^^$tag"""
      case ExpressionF.LANG_STRING(s, tag) => s"""\"$s\"@$tag"""
      case ExpressionF.NUM(s)              => s
      case ExpressionF.VARIABLE(s)         => s
      case ExpressionF.URIVAL(s)           => s
      case ExpressionF.BLANK(s)            => s
      case ExpressionF.BOOL(s)             => s
      case ASC(e)                          => s"(asc $e)"
      case DESC(e)                         => s"(desc $e)"
      case UUID()                          => "(uuid)"
      case CEIL(e)                         => s"(ceil $e)"
      case ROUND(e)                        => s"(round $e)"
      case RAND()                          => "(rand)"
      case ABS(e)                          => s"(abs $e)"
      case FLOOR(e)                        => s"(floor $e)"
      case STRUUID()                       => "(struuid)"
    }

  private val exprToString: Algebra[ExprF, String] =
    Algebra {
      case ExtendF(bindTo, bindFrom, r) =>
        s"(extend ((${bindTo.s} ${printExpression(bindFrom)})) $r)"
      case FilteredLeftJoinF(l, r, f) =>
        s"(optional $l $r (filter ${f.map(printExpression).mkString(", ")}))"
      case UnionF(l, r)          => s"(union $l $r)"
      case BGPF(quads)           => "(bgp " ++ quads.map(printQuad).mkString("\n") ++ ")"
      case GraphF(g, e)          => s"(graph <${getCleanUri(g)}> $e)"
      case JoinF(l, r)           => s"(join $l $r)"
      case LeftJoinF(l, r)       => s"(leftjoin $l $r)"
      case ProjectF(vars, r)     => r
      case QuadF(s, p, o, g)     => s"(quadf s p o g)"
      case DistinctF(r)          => s"(distinct $r)"
      case ReducedF(r)           => s"(reduced $r)"
      case GroupF(vars, func, r) => s"(group (${printStringVal(vars)}) $r)"
      case OrderF(conds, r) =>
        s"(order (${conds.asInstanceOf[Seq[Expression]].map(printExpression).mkString(" ")}) $r)"
      case OffsetLimitF(None, None, r)       => r
      case OffsetLimitF(None, Some(l), r)    => s"(slice _ $l $r)"
      case OffsetLimitF(Some(o), None, r)    => s"(slice $o _ $r)"
      case OffsetLimitF(Some(o), Some(l), r) => s"(slice $o $l $r)"
      case FilterF(funcs, expr) =>
        s"(filter (${funcs.map(printExpression).mkString(" ")}) $expr)"
      case TableF(vars, rows) => s"(tablef vars, rows)"
      case RowF(tuples)       => s"(rowf tuples)"
      case TabUnitF()         => s"(tabunitf )"
      case MinusF(l, r)       => s"(minus $l $r)"
      case OpNilF()           => s"(opnilf )"
      case ExistsF(not, p, r) =>
        val n = if (not) "notexists" else "exists"
        s"($n $p $r)"
    }

  private def extractQueryParams(uri: URI): List[QueryParam] =
    uri.getQuery
      .split("&")
      .map { qp =>
        val arr = qp.split("=")
        QueryParam(arr(0), arr(1))
      }
      .toList

}
