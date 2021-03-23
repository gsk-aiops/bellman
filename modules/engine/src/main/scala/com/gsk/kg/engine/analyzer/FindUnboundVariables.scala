package com.gsk.kg.engine
package analyzer

import DAG._

import cats._
import cats.data.State
import cats.data.Validated._
import cats.data.ValidatedNec
import cats.implicits._

import com.gsk.kg.engine.data.ChunkedList
import com.gsk.kg.sparqlparser.StringVal.VARIABLE

import higherkindness.droste.syntax.all._
import higherkindness.droste.{Project => _, _}

import optics._
import org.apache.spark.sql.DataFrame

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
    val findUnboundVariables: AlgebraM[ST, DAG, Set[VARIABLE]] =
      AlgebraM[ST, DAG, Set[VARIABLE]] {
        case Describe(vars, r) => (vars.toSet diff r).pure[ST]
        case Ask(r)            => Set.empty.pure[ST]
        case Construct(bgp, r) =>
          val used = bgp.quads
            .flatMap(_.getVariables)
            .map(_._1.asInstanceOf[VARIABLE])
            .toSet
          for {
            declared <- State.get
          } yield (used diff declared) ++ r
        case Scan(graph, expr) =>
          Set.empty.pure[ST]
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
            .flatMap(_ => Set.empty.pure[ST])
        case LeftJoin(l, r, filters) => (l union r).pure[ST]
        case Union(l, r)             => (l union r).pure[ST]
        case Filter(funcs, expr)     => expr.pure[ST]
        case Join(l, r)              => r.pure[ST]
        case Offset(offset, r)       => r.pure[ST]
        case Limit(limit, r)         => r.pure[ST]
        case Distinct(r)             => r.pure[ST]
        case Noop(trace)             => Set.empty.pure[ST]
      }

    val analyze = scheme.cataM[ST, DAG, T, Set[VARIABLE]](findUnboundVariables)

    val unbound: Set[VARIABLE] = analyze(t).runA(Set.empty).value

    if (unbound.nonEmpty) {
      val msg = "found free variables " + unbound.mkString(", ")

      msg.invalidNec
    } else {
      "ok".validNec
    }
  }
}
