package com.gsk.kg.engine
package analyzer

import cats.data.State
import cats.implicits._
import cats.{Group => _, _}

import higherkindness.droste.{Project => _, _}

import com.gsk.kg.engine.DAG._
import com.gsk.kg.engine.ExpressionF._
import com.gsk.kg.engine.data.ChunkedList
import com.gsk.kg.sparqlparser.Expression
import com.gsk.kg.sparqlparser.StringVal.GRAPH_VARIABLE
import com.gsk.kg.sparqlparser.StringVal.VARIABLE

/** This rule performs a bottom-up traverse of the DAG (with a
  * [[higherkindness.droste.AlgebraM]]), accumulating bound variables
  * in the [[cats.data.State]].
  *
  * When arriving to the nodes that may use unbound variables
  * ([[DAG.Project]] and [[DAG.Construct]]), it compares the variables
  * used in that node with those that were declared beforehand in the
  * graph.  If there are any that are used but not declared, they're
  * returned and later on reported.
  */
object FindUnboundVariables {

  type ST[A] = State[Set[VARIABLE], A]

  def apply[T](implicit T: Basis[DAG, T]): Rule[T] = { t =>
    val analyze = scheme.cataM[ST, DAG, T, Set[VARIABLE]](findUnboundVariables)

    val unbound: Set[VARIABLE] = analyze(t)
      .runA(Set.empty)
      .value
      .filterNot(_ == VARIABLE(GRAPH_VARIABLE.s))

    if (unbound.nonEmpty) {
      val msg = "found free variables " + unbound.mkString(", ")

      msg.invalidNec
    } else {
      "ok".validNec
    }
  }

  val findUnboundVariables: AlgebraM[ST, DAG, Set[VARIABLE]] =
    AlgebraM[ST, DAG, Set[VARIABLE]] {
      case Describe(vars, r) => (vars.toSet diff r).pure[ST]
      case Ask(r)            => Set.empty[VARIABLE].pure[ST]
      case Construct(bgp, r) =>
        val used = bgp.quads
          .flatMap(_.getVariables)
          .map(_._1.asInstanceOf[VARIABLE])
          .toSet
        for {
          declared <- State.get
        } yield (used diff declared) ++ r
      case Scan(graph, expr) =>
        State
          .modify[Set[VARIABLE]](x => x + VARIABLE(graph))
          .flatMap(_ => expr.pure[ST])
      case Project(variables, r) =>
        for {
          declared <- State.get
        } yield (variables.toSet diff declared) ++ r
      case Bind(variable, expression, r) =>
        State
          .modify[Set[VARIABLE]](x => x + variable)
          .flatMap(_ => r.pure[ST])
      case BGP(triples) =>
        val vars = Traverse[ChunkedList]
          .toList(triples)
          .flatMap(_.getVariables)
          .map(_._1.asInstanceOf[VARIABLE])
          .toSet

        State
          .modify[Set[VARIABLE]](x => x union vars)
          .flatMap(_ => Set.empty[VARIABLE].pure[ST])
      case LeftJoin(l, r, filters) => (l union r).pure[ST]
      case Union(l, r)             => (l union r).pure[ST]
      case Filter(funcs, expr) =>
        val funcsVars = funcs.toList.toSet
          .foldLeft(Set.empty[VARIABLE]) { case (acc, func) =>
            acc ++ FindVariablesOnExpression.apply[Expression](func)
          }
        for {
          declared <- State.get
        } yield (funcsVars diff declared) ++ expr
      case Join(l, r)        => (l union r).pure[ST]
      case Offset(offset, r) => r.pure[ST]
      case Limit(limit, r)   => r.pure[ST]
      case Distinct(r)       => r.pure[ST]
      case Group(vars, func, r) =>
        for {
          declared <- State.get
        } yield (vars.toSet diff declared) ++ r
      case DAG.Order(conds, r) =>
        val condVars = conds.toList.toSet
          .foldLeft(Set.empty[VARIABLE]) { case (acc, cond) =>
            acc ++ FindVariablesOnExpression.apply[Expression](cond)
          }
        for {
          declared <- State.get
        } yield (condVars diff declared) ++ r
      case Noop(trace) => Set.empty[VARIABLE].pure[ST]
    }
}

object FindVariablesOnExpression {

  def apply[T](t: T)(implicit T: Basis[ExpressionF, T]): Set[VARIABLE] = {
    val algebra: Algebra[ExpressionF, Set[VARIABLE]] =
      Algebra[ExpressionF, Set[VARIABLE]] {
        case EQUALS(l, r)                    => l ++ r
        case REGEX(s, pattern, flags)        => s
        case STRENDS(s, f)                   => s
        case STRSTARTS(s, f)                 => s
        case GT(l, r)                        => l ++ r
        case LT(l, r)                        => l ++ r
        case GTE(l, r)                       => l ++ r
        case LTE(l, r)                       => l ++ r
        case OR(l, r)                        => l ++ r
        case AND(l, r)                       => l ++ r
        case NEGATE(s)                       => s
        case URI(s)                          => s
        case CONCAT(appendTo, append)        => appendTo ++ append.toList.toSet.flatten
        case STR(s)                          => s
        case STRAFTER(s, f)                  => s
        case STRBEFORE(s, f)                 => s
        case STRDT(s, uri)                   => s
        case SUBSTR(s, pos, len)             => s
        case ISBLANK(s)                      => s
        case REPLACE(st, pattern, by, flags) => st
        case COUNT(e)                        => e
        case SUM(e)                          => e
        case MIN(e)                          => e
        case MAX(e)                          => e
        case AVG(e)                          => e
        case SAMPLE(e)                       => e
        case GROUP_CONCAT(e, separator)      => e
        case STRING(s)                       => Set.empty[VARIABLE]
        case DT_STRING(s, tag)               => Set.empty[VARIABLE]
        case LANG_STRING(s, tag)             => Set.empty[VARIABLE]
        case NUM(s)                          => Set.empty[VARIABLE]
        case ExpressionF.VARIABLE(s)         => Set(VARIABLE(s))
        case URIVAL(s)                       => Set.empty[VARIABLE]
        case BLANK(s)                        => Set.empty[VARIABLE]
        case BOOL(s)                         => Set.empty[VARIABLE]
        case ASC(e)                          => e
        case DESC(e)                         => e
      }

    val eval =
      scheme.cata[ExpressionF, T, Set[VARIABLE]](algebra)

    eval(t)
  }
}
