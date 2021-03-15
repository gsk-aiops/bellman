package com.gsk.kg.engine
package data

import higherkindness.droste.scheme
import higherkindness.droste.Basis
import higherkindness.droste.Algebra
import cats.implicits._
import cats.Show
import com.gsk.kg.sparqlparser.Expr
import scala.collection.immutable.Nil
import cats.syntax.nonEmptyTraverse
import cats.data.{NonEmptyChain, NonEmptyList}
import cats.Traverse

/**
  * Typeclass that allows you converting values of type T to
  * [[TreeRep]].  The benefit of doing so is that we'll be able to
  * render them nicely wit the drawTree method.
  */
trait ToTree[T] {
  def toTree(t: T): TreeRep[String]
}

object ToTree extends LowPriorityToTreeInstances0 {

  def apply[T](implicit T: ToTree[T]): ToTree[T] = T

  implicit class ToTreeOps[T](private val t: T)(implicit T: ToTree[T]) {
    def toTree: TreeRep[String] = ToTree[T].toTree(t)
  }

  implicit def dagToTree[T: Basis[DAG, *]]: ToTree[T] =
    new ToTree[T] {
      def toTree(tree: T): TreeRep[String] = {
        import TreeRep._
        val alg = Algebra[DAG, TreeRep[String]] {
          case DAG.Describe(vars, r) =>
            Node("Describe", vars.map(_.s.toTree).toStream #::: Stream(r))
          case DAG.Ask(r) => Node("Ask", Stream(r))
          case DAG.Construct(bgp, r) =>
            Node(
              "Construct",
              Stream(DAG.fromExpr.apply(bgp.asInstanceOf[Expr]).toTree, r)
            )
          case DAG.Scan(graph, expr) => Node("Scan", Stream(graph.toTree, expr))
          case DAG.Project(variables, r) =>
            Node("Project", Stream(Leaf(variables.toString), r))
          case DAG.Bind(variable, expression, r) =>
            Node(
              "Bind",
              Stream(Leaf(variable.toString), expression.toTree, r)
            )
          case DAG.Triple(s, p, o) =>
            Node(s"Triple", Stream(s.s.toTree, p.s.toTree, o.s.toTree))
          case DAG.BGP(triples) => Node("BGP", Traverse[ChunkedList].toList(triples).toStream)
          case DAG.LeftJoin(l, r, filters) =>
            Node("LeftJoin", Stream(l, r) #::: filters.map(_.toTree).toStream)
          case DAG.Union(l, r) => Node("Union", Stream(l, r))
          case DAG.Filter(funcs, expr) =>
            Node("Filter", funcs.map(_.toTree).toList.toStream #::: Stream(expr))
          case DAG.Join(l, r) => Node("Join", Stream(l, r))
          case DAG.Offset(offset, r) =>
            Node(
              "Offset",
              Stream(offset.toTree, r)
            )
          case DAG.Limit(limit, r) =>
            Node(
              "Limit",
              Stream(limit.toTree, r)
            )
          case DAG.Distinct(r) => Node("Distinct", Stream(r))
          case DAG.Noop(str)   => Leaf(s"Noop($str)")
        }

        val t = scheme.cata(alg)

        t(tree)
      }
    }

  implicit def expressionfToTree[T: Basis[ExpressionF, *]]: ToTree[T] =
    new ToTree[T] {
      def toTree(tree: T): TreeRep[String] = {
        import TreeRep._
        val alg = Algebra[ExpressionF, TreeRep[String]] {
          case ExpressionF.EQUALS(l, r)    => Node("EQUALS", Stream(l, r))
          case ExpressionF.GT(l, r)        => Node("GT", Stream(l, r))
          case ExpressionF.LT(l, r)        => Node("LT", Stream(l, r))
          case ExpressionF.OR(l, r)        => Node("OR", Stream(l, r))
          case ExpressionF.AND(l, r)       => Node("AND", Stream(l, r))
          case ExpressionF.NEGATE(s)       => Node("NEGATE", Stream(s))
          case ExpressionF.REGEX(l, r)     => Node("REGEX", Stream(l, r))
          case ExpressionF.STRSTARTS(l, r) => Node("STRSTARTS", Stream(l, r))
          case ExpressionF.URI(s)          => Node("URI", Stream(s))
          case ExpressionF.CONCAT(appendTo, append) =>
            Node("CONCAT", Stream(appendTo, append))
          case ExpressionF.STR(s) => Node("STR", Stream(s))
          case ExpressionF.STRAFTER(s, f) =>
            Node("STRAFTER", Stream(s, Leaf(f.toString)))
          case ExpressionF.ISBLANK(s) => Node("ISBLANK", Stream(s))
          case ExpressionF.REPLACE(st, pattern, by) =>
            Node("REPLACE", Stream(st, Leaf(pattern), Leaf(by)))

          case ExpressionF.STRING(s)   => Leaf(s"STRING($s)")
          case ExpressionF.NUM(s)      => Leaf(s"NUM($s)")
          case ExpressionF.VARIABLE(s) => Leaf(s"VARIABLE($s)")
          case ExpressionF.URIVAL(s)   => Leaf(s"URIVAL($s)")
          case ExpressionF.BLANK(s)    => Leaf(s"BLANK($s)")
          case ExpressionF.BOOL(s)     => Leaf(s"BOOL($s)")

        }

        val t = scheme.cata(alg)

        t(tree)
      }

    }

  implicit def listToTree[A: ToTree]: ToTree[List[A]] =
    new ToTree[List[A]] {
      def toTree(t: List[A]): TreeRep[String] =
        t match {
	        case Nil => TreeRep.Leaf("List.empty")
	        case nonempty =>
            TreeRep.Node("List", nonempty.map(_.toTree).toStream)
        }
    }

  implicit def nelToTree[A: ToTree]: ToTree[NonEmptyList[A]] =
    new ToTree[NonEmptyList[A]] {
      def toTree(t: NonEmptyList[A]): TreeRep[String] =
        TreeRep.Node("NonEmptyList", t.map(_.toTree).toList.toStream)
    }

  implicit def necToTree[A: ToTree]: ToTree[NonEmptyChain[A]] =
    new ToTree[NonEmptyChain[A]] {
      def toTree(t: NonEmptyChain[A]): TreeRep[String] =
        TreeRep.Node("NonEmptyChain", t.map(_.toTree).toList.toStream)
    }
}

trait LowPriorityToTreeInstances0 {

  // Instance of ToTree for anything that has a Show
  implicit def showToTree[T: Show]: ToTree[T] =
    new ToTree[T] {
      def toTree(t: T): TreeRep[String] = TreeRep.Leaf(Show[T].show(t))
    }

}
